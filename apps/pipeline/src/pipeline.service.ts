import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { loadConfig } from './config';
import { DbService } from './db.service';
import { ElasticsearchSink } from './elasticsearch.sink';
import { RabbitSink } from './rabbit.sink';
import { RedisService } from './redis.service';
import { RecordRow, toDocument } from './types';

type Mode = 'backfill' | 'incremental';

@Injectable()
export class PipelineService implements OnModuleInit {
  private readonly log = new Logger(PipelineService.name);
  private backfillRunning = false;
  private incrementalRunning = false;
  private backfillInFlight = 0;
  private incrementalInFlight = 0;
  private lastReadBackfillId = 0;
  private lastReadIncremental = { id: 0, ts: new Date(0) };
  private backoffMs = 1000;
  private settings = { batchSize: 500, maxInFlight: 2 };
  private recentChanges: Array<{ at: string; record_id: number; version: number; mode: Mode }> = [];
  private resumedFrom = { backfill: 0, incremental: 0 };
  private backfillLoop: Promise<void> | null = null;
  private incrementalLoop: Promise<void> | null = null;

  constructor(
    private readonly db: DbService,
    private readonly redis: RedisService,
    private readonly es: ElasticsearchSink,
    private readonly rabbit: RabbitSink,
  ) {}

  async onModuleInit() {
    const cfg = loadConfig();
    this.settings.batchSize = cfg.batchSize;
    this.settings.maxInFlight = cfg.maxInFlight;
    this.backoffMs = cfg.backoffStartMs;

    const backfill = await this.db.getCheckpoint('backfill');
    let incremental = await this.db.getCheckpoint('incremental');
    // Do not double-scan history: backfill owns id-ordered copy; incremental starts at boot.
    // Rows changed/inserted after this watermark are still picked up (SPEC overlap rule).
    const epoch = new Date('1970-01-01T00:00:00.000Z').getTime();
    const ts = incremental.cursor_ts ? new Date(incremental.cursor_ts).getTime() : epoch;
    if (Number(incremental.cursor_id) === 0 && ts <= epoch) {
      const bootWatermark = new Date();
      await this.db.saveCheckpoint('incremental', 0, bootWatermark);
      incremental = await this.db.getCheckpoint('incremental');
      this.log.log(`incremental watermark initialized to ${bootWatermark.toISOString()}`);
    }
    this.resumedFrom = {
      backfill: Number(backfill.cursor_id),
      incremental: Number(incremental.cursor_id),
    };
    this.log.log(`resumed backfill=${this.resumedFrom.backfill} incremental=${this.resumedFrom.incremental}`);

    // Auto-start both modes after boot (verify + UI can also control).
    setTimeout(() => {
      void this.startBackfill();
      void this.startIncremental();
    }, 2000);

    setInterval(() => void this.ensureLeader(), 3000);
  }

  private async ensureLeader() {
    if (!this.redis.getLeader()) {
      await this.redis.tryAcquireLeader();
    }
  }

  getState() {
    return {
      backfillRunning: this.backfillRunning,
      incrementalRunning: this.incrementalRunning,
      lastReadBackfillId: this.lastReadBackfillId,
      lastReadIncremental: this.lastReadIncremental,
      backoffMs: this.backoffMs,
      settings: this.settings,
      inFlight: {
        backfill: this.backfillInFlight,
        incremental: this.incrementalInFlight,
      },
      resumedFrom: this.resumedFrom,
    };
  }

  getRecentChanges(limit = 50) {
    return this.recentChanges.slice(0, limit);
  }

  private pushChange(row: RecordRow, mode: Mode) {
    this.recentChanges.unshift({
      at: new Date().toISOString(),
      record_id: Number(row.id),
      version: Number(row.version),
      mode,
    });
    if (this.recentChanges.length > 200) this.recentChanges.length = 200;
  }

  async startBackfill() {
    this.backfillRunning = true;
    if (!this.backfillLoop) {
      this.backfillLoop = this.loop('backfill').finally(() => {
        this.backfillLoop = null;
      });
    }
    return { ok: true };
  }

  async stopBackfill() {
    this.backfillRunning = false;
    return { ok: true };
  }

  async startIncremental() {
    this.incrementalRunning = true;
    if (!this.incrementalLoop) {
      this.incrementalLoop = this.loop('incremental').finally(() => {
        this.incrementalLoop = null;
      });
    }
    return { ok: true };
  }

  async stopIncremental() {
    this.incrementalRunning = false;
    return { ok: true };
  }

  updateSettings(partial: { batchSize?: number; maxInFlight?: number }) {
    if (partial.batchSize && partial.batchSize > 0) this.settings.batchSize = partial.batchSize;
    if (partial.maxInFlight && partial.maxInFlight > 0) this.settings.maxInFlight = partial.maxInFlight;
    return this.settings;
  }

  private running(mode: Mode) {
    return mode === 'backfill' ? this.backfillRunning : this.incrementalRunning;
  }

  private getInFlight(mode: Mode) {
    return mode === 'backfill' ? this.backfillInFlight : this.incrementalInFlight;
  }

  private setInFlight(mode: Mode, n: number) {
    if (mode === 'backfill') this.backfillInFlight = n;
    else this.incrementalInFlight = n;
  }

  private async loop(mode: Mode) {
    const cfg = loadConfig();
    this.log.log(`${mode} loop started`);
    while (this.running(mode)) {
      try {
        if (!this.redis.getLeader()) {
          await this.redis.tryAcquireLeader();
          await sleep(500);
          continue;
        }

        if (this.getInFlight(mode) >= this.settings.maxInFlight) {
          await sleep(50);
          continue;
        }

        const circuit = await this.redis.getCircuit();
        if (circuit.open) {
          await this.redis.incrCounter('backoff_sleeps');
          await sleep(this.backoffMs);
          const available = await this.es.isAvailable();
          if (available) {
            await this.redis.setCircuit(false);
            this.backoffMs = cfg.backoffStartMs;
            this.log.log('circuit closed — elasticsearch recovered');
          } else {
            this.backoffMs = Math.min(cfg.backoffCapMs, Math.floor(this.backoffMs * 1.5 + Math.random() * 200));
            continue;
          }
        }

        this.setInFlight(mode, this.getInFlight(mode) + 1);
        try {
          const progressed = await this.processOneBatch(mode);
          if (!progressed) {
            await sleep(mode === 'backfill' ? 1000 : 500);
          }
        } finally {
          this.setInFlight(mode, Math.max(0, this.getInFlight(mode) - 1));
        }
      } catch (e) {
        this.log.error(`${mode} loop error: ${(e as Error).message}`);
        await sleep(1000);
      }
    }
    this.log.log(`${mode} loop stopped`);
  }

  private async processOneBatch(mode: Mode): Promise<boolean> {
    const cfg = loadConfig();
    const cp = await this.db.getCheckpoint(mode);
    let rows: RecordRow[];

    if (mode === 'backfill') {
      rows = await this.db.fetchBackfillBatch(Number(cp.cursor_id), this.settings.batchSize);
      if (rows.length) this.lastReadBackfillId = Number(rows[rows.length - 1].id);
    } else {
      const ts = cp.cursor_ts ?? new Date(0);
      rows = await this.db.fetchIncrementalBatch(ts, Number(cp.cursor_id), this.settings.batchSize);
      if (rows.length) {
        const last = rows[rows.length - 1];
        this.lastReadIncremental = { id: Number(last.id), ts: last.updated_at };
      }
    }

    if (!rows.length) return false;

    const batchId = randomUUID();
    const esResult = await this.es.bulkUpsert(rows);

    if (esResult.transportError) {
      await this.redis.setCircuit(true, esResult.transportError);
      await this.redis.incrCounter('es_transport_errors');
      this.backoffMs = Math.min(cfg.backoffCapMs, Math.max(cfg.backoffStartMs, this.backoffMs));
      this.log.warn(`ES transport error (batch ${batchId}): ${esResult.transportError}`);
      // Do NOT advance checkpoint — at-least-once retry of whole batch.
      return true;
    }

    if (esResult.results.length !== rows.length) {
      await this.redis.setCircuit(true, 'bulk result length mismatch');
      await this.redis.incrCounter('es_transport_errors');
      this.log.warn(`ES bulk length mismatch batch=${batchId} results=${esResult.results.length} rows=${rows.length}`);
      return true;
    }

    // Per-item: successes stay; failures → DLQ; cursor advances.
    for (const item of esResult.results) {
      if (!item.ok) {
        const row = rows.find((r) => Number(r.id) === item.id)!;
        await this.db.insertDlq({
          recordId: Number(row.id),
          version: Number(row.version),
          batchId,
          sink: 'elasticsearch',
          payload: toDocument(row),
          error: item.error ?? 'unknown',
        });
        await this.redis.incrCounter('dlq_written');
      }
    }

    const rabbit = await this.rabbit.publishBatch(rows);
    if (!rabbit.ok) {
      // Rabbit failure: do not advance — retry whole batch (ES upserts are idempotent).
      await this.redis.incrCounter('rabbit_errors');
      this.log.warn(`Rabbit publish failed: ${rabbit.error}`);
      await sleep(500);
      return true;
    }

    const last = rows[rows.length - 1];
    if (mode === 'backfill') {
      await this.db.saveCheckpoint('backfill', Number(last.id), null);
    } else {
      await this.db.saveCheckpoint('incremental', Number(last.id), last.updated_at);
    }

    await this.redis.recordThroughput(rows.length);
    await this.redis.incrCounter('records_processed', rows.length);
    for (const row of rows) this.pushChange(row, mode);
    if (Number(last.id) % 10000 < this.settings.batchSize) {
      this.log.log(`${mode} checkpoint id=${last.id} batch=${rows.length}`);
    }
    return true;
  }

  async replayDlq(id: number) {
    const row = await this.db.getDlq(id);
    if (!row) return { ok: false, error: 'not found' };
    if (row.status !== 'open') return { ok: false, error: `status=${row.status}` };

    const payload = row.payload as unknown as RecordRow & {
      id: number;
      version: number;
      poison?: boolean;
      updated_at: string;
      created_at: string;
    };

    // Clear poison flag on replay so a human can recover after fixing data,
    // OR re-fetch from source if present.
    const source = await this.db.getRecord(Number(row.record_id));
    const toSend: RecordRow = source
      ? { ...source, poison: false }
      : {
          id: Number(payload.id),
          email: String((payload as { email?: string }).email ?? ''),
          name: String((payload as { name?: string }).name ?? ''),
          payload: (payload as { payload?: Record<string, unknown> }).payload ?? {},
          version: Number(row.version),
          poison: false,
          updated_at: new Date(payload.updated_at ?? Date.now()),
          created_at: new Date(payload.created_at ?? Date.now()),
        };

    const es = await this.es.bulkUpsert([toSend]);
    if (es.transportError) return { ok: false, error: es.transportError };
    if (es.results[0] && !es.results[0].ok) return { ok: false, error: es.results[0].error };

    await this.rabbit.publishBatch([toSend]);
    await this.db.markDlqReplayed(id);
    return { ok: true };
  }
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
