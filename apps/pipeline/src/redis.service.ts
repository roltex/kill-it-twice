import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { loadConfig } from './config';

const LOCK_KEY = 'pipeline:leader';
const LOCK_TTL_MS = 5000;
const THROUGHPUT_KEY = 'pipeline:throughput';
const CIRCUIT_KEY = 'pipeline:circuit:elasticsearch';
const SIM_ES_DOWN = 'pipeline:sim:es_down';
const COUNTERS_KEY = 'pipeline:counters';

@Injectable()
export class RedisService implements OnModuleInit, OnModuleDestroy {
  private client!: Redis;
  private lockToken = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  private renewTimer: NodeJS.Timeout | null = null;
  private isLeader = false;

  async onModuleInit() {
    const cfg = loadConfig();
    this.client = new Redis(cfg.redisUrl, { maxRetriesPerRequest: null, enableReadyCheck: true });
    await this.client.ping();
  }

  async onModuleDestroy() {
    if (this.renewTimer) clearInterval(this.renewTimer);
    if (this.isLeader) {
      const cur = await this.client.get(LOCK_KEY);
      if (cur === this.lockToken) await this.client.del(LOCK_KEY);
    }
    await this.client.quit();
  }

  async tryAcquireLeader(): Promise<boolean> {
    const ok = await this.client.set(LOCK_KEY, this.lockToken, 'PX', LOCK_TTL_MS, 'NX');
    this.isLeader = ok === 'OK';
    if (this.isLeader && !this.renewTimer) {
      this.renewTimer = setInterval(() => {
        void this.renewLeader();
      }, 2000);
    }
    return this.isLeader;
  }

  private async renewLeader() {
    const script = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("pexpire", KEYS[1], ARGV[2])
      else
        return 0
      end
    `;
    const res = await this.client.eval(script, 1, LOCK_KEY, this.lockToken, String(LOCK_TTL_MS));
    this.isLeader = Number(res) === 1;
  }

  getLeader(): boolean {
    return this.isLeader;
  }

  async recordThroughput(n: number) {
    const now = Math.floor(Date.now() / 1000);
    await this.client.multi().incrby(`${THROUGHPUT_KEY}:${now}`, n).expire(`${THROUGHPUT_KEY}:${now}`, 120).exec();
  }

  async getThroughputPerSec(): Promise<number> {
    const now = Math.floor(Date.now() / 1000);
    const vals = await this.client.mget(`${THROUGHPUT_KEY}:${now}`, `${THROUGHPUT_KEY}:${now - 1}`);
    const a = Number(vals[0] ?? 0);
    const b = Number(vals[1] ?? 0);
    return a > 0 ? a : b;
  }

  async setCircuit(open: boolean, reason?: string) {
    if (open) {
      await this.client.hset(CIRCUIT_KEY, { open: '1', reason: reason ?? 'sink_down', since: String(Date.now()) });
    } else {
      await this.client.hset(CIRCUIT_KEY, { open: '0', reason: '', since: '' });
    }
  }

  async getCircuit(): Promise<{ open: boolean; reason: string; since: number | null }> {
    const data = await this.client.hgetall(CIRCUIT_KEY);
    return {
      open: data.open === '1',
      reason: data.reason ?? '',
      since: data.since ? Number(data.since) : null,
    };
  }

  async incrCounter(name: string, by = 1) {
    await this.client.hincrby(COUNTERS_KEY, name, by);
  }

  async getCounters(): Promise<Record<string, number>> {
    const data = await this.client.hgetall(COUNTERS_KEY);
    const out: Record<string, number> = {};
    for (const [k, v] of Object.entries(data)) out[k] = Number(v);
    return out;
  }

  async setSimEsDown(down: boolean) {
    if (down) await this.client.set(SIM_ES_DOWN, '1');
    else await this.client.del(SIM_ES_DOWN);
  }

  async isSimEsDown(): Promise<boolean> {
    return (await this.client.get(SIM_ES_DOWN)) === '1';
  }
}
