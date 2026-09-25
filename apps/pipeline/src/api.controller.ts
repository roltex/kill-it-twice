import { Body, Controller, Get, Param, Post, Query, Res } from '@nestjs/common';
import { Response } from 'express';
import { DbService } from './db.service';
import { ElasticsearchSink } from './elasticsearch.sink';
import { PipelineService } from './pipeline.service';
import { RedisService } from './redis.service';

@Controller()
export class ApiController {
  constructor(
    private readonly db: DbService,
    private readonly redis: RedisService,
    private readonly es: ElasticsearchSink,
    private readonly pipeline: PipelineService,
  ) {}

  @Get('health')
  health() {
    return { ok: true };
  }

  @Get('api/status')
  async status() {
    return this.buildStatus();
  }

  @Get('api/status.txt')
  async statusText(@Res() res: Response) {
    const s = await this.buildStatus();
    const lines = [
      `backfill_cursor=${s.backfill.cursor_id}`,
      `last_read=${s.backfill.last_read_id}`,
      `max_id=${s.backfill.max_id}`,
      `resumed_from=${s.resumed_from.backfill}`,
      `throughput=${s.throughput_per_sec}`,
      `lag_count=${s.incremental.lag_count}`,
      `lag_seconds=${s.incremental.lag_seconds}`,
      `dlq_open=${s.dlq_open}`,
      `circuit_open=${s.circuit.open ? 1 : 0}`,
      `backoff_sleeps=${s.counters.backoff_sleeps ?? 0}`,
      `es_transport_errors=${s.counters.es_transport_errors ?? 0}`,
      `health=${s.health}`,
      `es_docs=${s.elasticsearch_docs}`,
      `source_count=${s.backfill.total_source}`,
      `backfill_running=${s.backfill.running ? 1 : 0}`,
      `guarantee=${s.guarantee}`,
    ];
    res.setHeader('Content-Type', 'text/plain');
    res.send(lines.join('\n') + '\n');
  }

  @Get('metrics')
  async metrics(@Res() res: Response) {
    const s = await this.buildStatus();
    const lines = [
      '# HELP optio_backfill_cursor Backfill checkpoint cursor id',
      '# TYPE optio_backfill_cursor gauge',
      `optio_backfill_cursor ${s.backfill.cursor_id}`,
      '# HELP optio_throughput_per_sec Recent throughput',
      '# TYPE optio_throughput_per_sec gauge',
      `optio_throughput_per_sec ${s.throughput_per_sec}`,
      '# HELP optio_incremental_lag_count Incremental lag in records',
      '# TYPE optio_incremental_lag_count gauge',
      `optio_incremental_lag_count ${s.incremental.lag_count}`,
      '# HELP optio_dlq_open Open DLQ entries',
      '# TYPE optio_dlq_open gauge',
      `optio_dlq_open ${s.dlq_open}`,
      '# HELP optio_circuit_open Elasticsearch circuit open (1/0)',
      '# TYPE optio_circuit_open gauge',
      `optio_circuit_open ${s.circuit.open ? 1 : 0}`,
      '# HELP optio_health 1=healthy 0=unhealthy',
      '# TYPE optio_health gauge',
      `optio_health ${s.healthy ? 1 : 0}`,
      '# HELP optio_backoff_sleeps Count of backoff sleeps during sink outage',
      '# TYPE optio_backoff_sleeps counter',
      `optio_backoff_sleeps ${s.counters.backoff_sleeps ?? 0}`,
      '# HELP optio_es_transport_errors Elasticsearch transport errors',
      '# TYPE optio_es_transport_errors counter',
      `optio_es_transport_errors ${s.counters.es_transport_errors ?? 0}`,
      '# HELP optio_records_processed Records processed through pipeline',
      '# TYPE optio_records_processed counter',
      `optio_records_processed ${s.counters.records_processed ?? 0}`,
    ];
    res.setHeader('Content-Type', 'text/plain; version=0.0.4');
    res.send(lines.join('\n') + '\n');
  }

  @Get('api/records')
  async records(@Query('q') q?: string) {
    try {
      const items = await this.es.search(q ?? '', 50);
      return { source: 'elasticsearch', items };
    } catch (e) {
      return { source: 'elasticsearch', items: [], error: (e as Error).message };
    }
  }

  @Get('api/records/:id')
  async record(@Param('id') id: string) {
    const num = Number(id);
    const [pg, es] = await Promise.all([this.db.getRecord(num), this.es.getById(num)]);
    return { postgres: pg, elasticsearch: es };
  }

  @Get('api/changes')
  changes(@Query('limit') limit?: string) {
    return { items: this.pipeline.getRecentChanges(Number(limit ?? 50)) };
  }

  @Post('api/control/backfill/start')
  startBackfill() {
    return this.pipeline.startBackfill();
  }

  @Post('api/control/backfill/stop')
  stopBackfill() {
    return this.pipeline.stopBackfill();
  }

  @Post('api/control/incremental/start')
  startIncremental() {
    return this.pipeline.startIncremental();
  }

  @Post('api/control/incremental/stop')
  stopIncremental() {
    return this.pipeline.stopIncremental();
  }

  @Post('api/control/settings')
  settings(@Body() body: { batchSize?: number; maxInFlight?: number }) {
    return this.pipeline.updateSettings(body ?? {});
  }

  @Get('api/dlq')
  dlq() {
    return this.db.listDlq(100);
  }

  @Post('api/dlq/:id/replay')
  replay(@Param('id') id: string) {
    return this.pipeline.replayDlq(Number(id));
  }

  @Post('api/simulate/search-sink')
  async simSink(@Body() body: { down?: boolean }) {
    const down = Boolean(body?.down);
    await this.redis.setSimEsDown(down);
    this.es.setForcedDown(down);
    if (down) await this.redis.setCircuit(true, 'simulated');
    else await this.redis.setCircuit(false);
    return { ok: true, down };
  }

  @Post('api/simulate/poison')
  async simPoison() {
    const id = await this.db.insertRecord({
      email: `poison-${Date.now()}@example.com`,
      name: 'Poison Record',
      poison: true,
    });
    return { ok: true, id };
  }

  @Post('api/simulate/changes')
  async simChanges(@Body() body: { count?: number }) {
    const n = await this.db.bumpRecords(body?.count ?? 10);
    return { ok: true, updated: n };
  }

  @Post('api/verify/poison-batch')
  async poisonBatch(@Body() body: { count?: number; poison?: number }) {
    const poison = body?.poison ?? 3;
    const ids = await this.db.insertPoisonBatch(body?.count ?? 500, poison);
    return { ok: true, ids, poison_ids: ids.slice(0, poison) };
  }

  @Post('api/admin/refresh')
  async refresh() {
    await this.es.refresh();
    return { ok: true, docs: await this.es.countDocs() };
  }

  private async buildStatus() {
    const backfill = await this.db.getCheckpoint('backfill');
    const incremental = await this.db.getCheckpoint('incremental');
    const total = await this.db.countRecords();
    const maxId = await this.db.maxRecordId();
    const dlq = await this.db.dlqOpenCount();
    const circuit = await this.redis.getCircuit();
    const throughput = await this.redis.getThroughputPerSec();
    const counters = await this.redis.getCounters();
    const state = this.pipeline.getState();
    const lagCount = await this.db.lagCount(
      incremental.cursor_ts ? new Date(incremental.cursor_ts) : null,
      Number(incremental.cursor_id),
    );
    const lagSeconds =
      lagCount === 0 || !incremental.cursor_ts
        ? 0
        : Math.max(0, (Date.now() - new Date(incremental.cursor_ts).getTime()) / 1000);

    const backfillPct = maxId > 0 ? (Number(backfill.cursor_id) / maxId) * 100 : 0;
    const healthy = !circuit.open;

    return {
      health: healthy ? 'green' : 'red',
      healthy,
      guarantee: 'effectively-once',
      resumed_from: state.resumedFrom,
      backfill: {
        running: state.backfillRunning,
        cursor_id: Number(backfill.cursor_id),
        last_read_id: state.lastReadBackfillId,
        percent: Number(backfillPct.toFixed(2)),
        total_source: total,
        max_id: maxId,
      },
      incremental: {
        running: state.incrementalRunning,
        cursor_id: Number(incremental.cursor_id),
        cursor_ts: incremental.cursor_ts,
        last_read: state.lastReadIncremental,
        lag_count: lagCount,
        lag_seconds: Number(lagSeconds.toFixed(1)),
      },
      throughput_per_sec: throughput,
      dlq_open: dlq,
      circuit,
      backoff_ms: state.backoffMs,
      in_flight: state.inFlight,
      settings: state.settings,
      counters,
      elasticsearch_docs: await this.es.countDocs(),
    };
  }
}
