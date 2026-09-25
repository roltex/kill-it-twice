import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Client } from '@elastic/elasticsearch';
import { loadConfig } from './config';
import { RecordRow, toDocument } from './types';
import { RedisService } from './redis.service';

export interface BulkItemResult {
  id: number;
  ok: boolean;
  error?: string;
}

@Injectable()
export class ElasticsearchSink implements OnModuleInit {
  private readonly log = new Logger(ElasticsearchSink.name);
  private client!: Client;
  private indexName!: string;
  private forcedDown = false;

  constructor(private readonly redis: RedisService) {}

  async onModuleInit() {
    const cfg = loadConfig();
    this.indexName = cfg.indexName;
    this.client = new Client({
      node: cfg.elasticsearchUrl,
      requestTimeout: 5000,
      pingTimeout: 2000,
      maxRetries: 0,
    });
    for (let attempt = 1; attempt <= 30; attempt++) {
      try {
        await this.ensureIndex();
        return;
      } catch (e) {
        this.log.warn(`Elasticsearch not ready (${attempt}/30): ${(e as Error).message}`);
        await new Promise((r) => setTimeout(r, 2000));
      }
    }
    throw new Error('Elasticsearch did not become ready');
  }

  private async ensureIndex() {
    try {
      await this.client.ingest.putPipeline({
        id: 'reject-poison',
        description: 'Reject poison records for G4 partial batch failure',
        processors: [
          {
            fail: {
              if: 'ctx.poison == true',
              message: 'poison record rejected by ingest pipeline',
            },
          },
        ],
      });
    } catch (e) {
      this.log.warn(`ingest pipeline setup: ${(e as Error).message}`);
    }

    const exists = await this.client.indices.exists({ index: this.indexName });
    if (!exists) {
      await this.client.indices.create({
        index: this.indexName,
        settings: {
          number_of_shards: 1,
          number_of_replicas: 0,
          refresh_interval: '5s',
          default_pipeline: 'reject-poison',
        },
        mappings: {
          properties: {
            id: { type: 'long' },
            email: { type: 'keyword' },
            name: { type: 'text', fields: { keyword: { type: 'keyword' } } },
            payload: { type: 'object', enabled: true },
            version: { type: 'long' },
            poison: { type: 'boolean' },
            updated_at: { type: 'date' },
            created_at: { type: 'date' },
          },
        },
      });
      this.log.log(`Created index ${this.indexName}`);
    }
  }

  setForcedDown(down: boolean) {
    this.forcedDown = down;
  }

  async isAvailable(): Promise<boolean> {
    if (this.forcedDown || (await this.redis.isSimEsDown())) return false;
    try {
      await this.client.ping();
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Index API + ingest pipeline so poison docs fail per item (G4).
   * external_gte version keeps replays idempotent and prevents older versions winning.
   * A version conflict means a newer or equal document is already stored — treat as success.
   */
  async bulkUpsert(rows: RecordRow[]): Promise<{ results: BulkItemResult[]; transportError?: string }> {
    if (this.forcedDown || (await this.redis.isSimEsDown())) {
      return { results: [], transportError: 'elasticsearch simulated down' };
    }

    const operations: object[] = [];
    for (const row of rows) {
      const doc = toDocument(row);
      operations.push({
        index: {
          _index: this.indexName,
          _id: String(doc.id),
          version: doc.version,
          version_type: 'external_gte',
          pipeline: 'reject-poison',
        },
      });
      operations.push(doc);
    }

    try {
      const res = await this.client.bulk({ refresh: false, operations });
      const results: BulkItemResult[] = [];
      const items = res.items ?? [];
      for (let i = 0; i < items.length; i++) {
        const item = items[i].index ?? items[i].update;
        const row = rows[i];
        if (!item) {
          results.push({ id: Number(row.id), ok: false, error: 'missing item result' });
          continue;
        }
        if (item.error) {
          const type = item.error.type ?? 'error';
          if (type === 'version_conflict_engine_exception') {
            results.push({ id: Number(row.id), ok: true });
          } else {
            results.push({
              id: Number(row.id),
              ok: false,
              error: `${type}: ${item.error.reason}`,
            });
          }
        } else {
          results.push({ id: Number(row.id), ok: true });
        }
      }
      return { results };
    } catch (e) {
      return { results: [], transportError: (e as Error).message };
    }
  }

  async refresh() {
    await this.client.indices.refresh({ index: this.indexName });
  }

  async countDocs(): Promise<number> {
    try {
      const res = await this.client.count({ index: this.indexName });
      return res.count;
    } catch {
      return -1;
    }
  }

  async getById(id: number) {
    try {
      const res = await this.client.get({ index: this.indexName, id: String(id) });
      return res._source;
    } catch {
      return null;
    }
  }

  async search(q: string, limit = 50) {
    const res = await this.client.search({
      index: this.indexName,
      size: limit,
      query: q
        ? {
            multi_match: {
              query: q,
              fields: ['email', 'name', 'id'],
            },
          }
        : { match_all: {} },
      sort: [{ id: 'desc' }],
    });
    return res.hits.hits.map((h) => h._source);
  }
}
