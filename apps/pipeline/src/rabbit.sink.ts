import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import amqp from 'amqplib';
import { loadConfig } from './config';
import { RecordRow, eventId, toDocument } from './types';

@Injectable()
export class RabbitSink implements OnModuleInit, OnModuleDestroy {
  private readonly log = new Logger(RabbitSink.name);
  // amqplib typings differ across versions; keep runtime shape, skip fragile Connection typing.
  private conn: any = null;
  private channel: any = null;
  private exchange!: string;
  private queue!: string;

  async onModuleInit() {
    const cfg = loadConfig();
    this.exchange = cfg.exchangeName;
    this.queue = cfg.queueName;
    for (let attempt = 1; attempt <= 30; attempt++) {
      try {
        await this.connect();
        return;
      } catch (e) {
        this.log.warn(`RabbitMQ not ready (${attempt}/30): ${(e as Error).message}`);
        await new Promise((r) => setTimeout(r, 2000));
      }
    }
    throw new Error('RabbitMQ did not become ready');
  }

  private async connect() {
    const cfg = loadConfig();
    this.conn = await amqp.connect(cfg.rabbitmqUrl);
    this.channel = await this.conn.createConfirmChannel();
    await this.channel.assertExchange(this.exchange, 'topic', { durable: true });
    await this.channel.assertQueue(this.queue, { durable: true });
    await this.channel.bindQueue(this.queue, this.exchange, 'record.#');
    this.log.log(`RabbitMQ ready exchange=${this.exchange} queue=${this.queue}`);
  }

  async onModuleDestroy() {
    try {
      await this.channel?.close();
      await this.conn?.close();
    } catch {
      /* ignore */
    }
  }

  async publishBatch(rows: RecordRow[]): Promise<{ ok: boolean; error?: string }> {
    try {
      for (const row of rows) {
        const body = Buffer.from(
          JSON.stringify({
            event_id: eventId(row.id, row.version),
            record_id: Number(row.id),
            version: Number(row.version),
            document: toDocument(row),
            emitted_at: new Date().toISOString(),
          }),
        );
        const ok = this.channel.publish(this.exchange, `record.${row.id}`, body, {
          persistent: true,
          messageId: eventId(row.id, row.version),
          contentType: 'application/json',
        });
        if (!ok) {
          await new Promise<void>((resolve) => this.channel.once('drain', () => resolve()));
        }
      }
      await this.channel.waitForConfirms();
      return { ok: true };
    } catch (e) {
      this.log.error(`publish failed: ${(e as Error).message}`);
      try {
        await this.connect();
      } catch {
        /* reconnect attempted */
      }
      return { ok: false, error: (e as Error).message };
    }
  }
}
