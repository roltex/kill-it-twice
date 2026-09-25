import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Pool, QueryResultRow } from 'pg';
import { loadConfig } from './config';
import { Checkpoint, DlqRow, RecordRow } from './types';

@Injectable()
export class DbService implements OnModuleInit, OnModuleDestroy {
  private pool!: Pool;

  async onModuleInit() {
    const cfg = loadConfig();
    this.pool = new Pool({ connectionString: cfg.databaseUrl, max: 10 });
    await this.pool.query('SELECT 1');
  }

  async onModuleDestroy() {
    await this.pool.end();
  }

  async query<T extends QueryResultRow = QueryResultRow>(sql: string, params: unknown[] = []) {
    return this.pool.query<T>(sql, params);
  }

  async getCheckpoint(mode: 'backfill' | 'incremental'): Promise<Checkpoint> {
    const res = await this.query<Checkpoint>(
      `SELECT mode, cursor_id, cursor_ts, updated_at FROM checkpoints WHERE mode = $1`,
      [mode],
    );
    return res.rows[0];
  }

  async saveCheckpoint(mode: 'backfill' | 'incremental', cursorId: number, cursorTs: Date | null) {
    await this.query(
      `UPDATE checkpoints
       SET cursor_id = $2, cursor_ts = $3, updated_at = NOW()
       WHERE mode = $1`,
      [mode, cursorId, cursorTs],
    );
  }

  async fetchBackfillBatch(afterId: number, limit: number): Promise<RecordRow[]> {
    const res = await this.query<RecordRow>(
      `SELECT id, email, name, payload, version, poison, updated_at, created_at
       FROM records
       WHERE id > $1
       ORDER BY id
       LIMIT $2`,
      [afterId, limit],
    );
    return res.rows;
  }

  async fetchIncrementalBatch(afterTs: Date, afterId: number, limit: number): Promise<RecordRow[]> {
    const res = await this.query<RecordRow>(
      `SELECT id, email, name, payload, version, poison, updated_at, created_at
       FROM records
       WHERE (updated_at, id) > ($1::timestamptz, $2::bigint)
       ORDER BY updated_at, id
       LIMIT $3`,
      [afterTs, afterId, limit],
    );
    return res.rows;
  }

  async countRecords(): Promise<number> {
    const res = await this.query<{ count: string }>(`SELECT COUNT(*)::text AS count FROM records`);
    return Number(res.rows[0].count);
  }

  async maxRecordId(): Promise<number> {
    const res = await this.query<{ max: string | null }>(`SELECT MAX(id)::text AS max FROM records`);
    return Number(res.rows[0].max ?? 0);
  }

  async lagCount(cursorTs: Date | null, cursorId: number): Promise<number> {
    if (!cursorTs) return 0;
    const res = await this.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM records
       WHERE (updated_at, id) > ($1::timestamptz, $2::bigint)`,
      [cursorTs, cursorId],
    );
    return Number(res.rows[0].count);
  }

  async insertDlq(entry: {
    recordId: number;
    version: number;
    batchId: string;
    sink: string;
    payload: unknown;
    error: string;
  }) {
    await this.query(
      `INSERT INTO dlq (record_id, version, batch_id, sink, payload, error, status)
       VALUES ($1, $2, $3, $4, $5::jsonb, $6, 'open')
       ON CONFLICT (record_id, version, sink) WHERE status = 'open' DO NOTHING`,
      [entry.recordId, entry.version, entry.batchId, entry.sink, JSON.stringify(entry.payload), entry.error],
    );
  }

  async listDlq(limit = 100): Promise<DlqRow[]> {
    const res = await this.query<DlqRow>(
      `SELECT id, record_id, version, batch_id, sink, payload, error, status, created_at, replayed_at
       FROM dlq ORDER BY id DESC LIMIT $1`,
      [limit],
    );
    return res.rows;
  }

  async getDlq(id: number): Promise<DlqRow | null> {
    const res = await this.query<DlqRow>(
      `SELECT id, record_id, version, batch_id, sink, payload, error, status, created_at, replayed_at
       FROM dlq WHERE id = $1`,
      [id],
    );
    return res.rows[0] ?? null;
  }

  async markDlqReplayed(id: number) {
    await this.query(`UPDATE dlq SET status = 'replayed', replayed_at = NOW() WHERE id = $1`, [id]);
  }

  async dlqOpenCount(): Promise<number> {
    const res = await this.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM dlq WHERE status = 'open'`,
    );
    return Number(res.rows[0].count);
  }

  async getRecord(id: number): Promise<RecordRow | null> {
    const res = await this.query<RecordRow>(
      `SELECT id, email, name, payload, version, poison, updated_at, created_at
       FROM records WHERE id = $1`,
      [id],
    );
    return res.rows[0] ?? null;
  }

  async searchRecords(q: string, limit = 50): Promise<RecordRow[]> {
    const res = await this.query<RecordRow>(
      `SELECT id, email, name, payload, version, poison, updated_at, created_at
       FROM records
       WHERE email ILIKE $1 OR name ILIKE $1 OR id::text = $2
       ORDER BY id DESC
       LIMIT $3`,
      [`%${q}%`, q, limit],
    );
    return res.rows;
  }

  async insertPoisonBatch(count: number, poisonCount: number): Promise<number[]> {
    const res = await this.query<{ id: string }>(
      `WITH inserted AS (
         INSERT INTO records (email, name, payload, version, poison, updated_at, created_at)
         SELECT
           'poison-batch-' || g || '-' || floor(extract(epoch from now()))::text || '@example.com',
           'PoisonBatch ' || g,
           jsonb_build_object('kind', 'g4', 'i', g),
           1,
           (g <= $2),
           NOW(),
           NOW()
         FROM generate_series(1, $1) AS g
         RETURNING id::text AS id, poison
       )
       SELECT id FROM inserted ORDER BY id::bigint`,
      [count, poisonCount],
    );
    return res.rows.map((r) => Number(r.id));
  }

  async bumpRecords(limit: number): Promise<number> {
    const res = await this.query<{ id: string }>(
      `UPDATE records SET version = version + 1, updated_at = NOW()
       WHERE id IN (SELECT id FROM records ORDER BY random() LIMIT $1)
       RETURNING id::text AS id`,
      [limit],
    );
    return res.rowCount ?? 0;
  }

  async insertRecord(input: { email: string; name: string; poison?: boolean }) {
    const res = await this.query<{ id: string }>(
      `INSERT INTO records (email, name, payload, version, poison)
       VALUES ($1, $2, '{}'::jsonb, 1, $3)
       RETURNING id::text AS id`,
      [input.email, input.name, input.poison ?? false],
    );
    return Number(res.rows[0].id);
  }
}
