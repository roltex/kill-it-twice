import amqp from 'amqplib';
import { Pool } from 'pg';

const databaseUrl = process.env.DATABASE_URL ?? 'postgres://optio:optio@localhost:5432/optio';
const rabbitUrl = process.env.RABBITMQ_URL ?? 'amqp://optio:optio@localhost:5672';
const queueName = process.env.QUEUE_NAME ?? 'optio.changes';
const exchangeName = process.env.EXCHANGE_NAME ?? 'optio.changes';

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const pool = new Pool({ connectionString: databaseUrl, max: 8 });
  for (let i = 1; i <= 30; i++) {
    try {
      await pool.query('SELECT 1');
      break;
    } catch (e) {
      console.log(`postgres not ready (${i}): ${(e as Error).message}`);
      await sleep(2000);
    }
  }

  let conn: Awaited<ReturnType<typeof amqp.connect>> | null = null;
  for (let i = 1; i <= 30; i++) {
    try {
      conn = await amqp.connect(rabbitUrl);
      break;
    } catch (e) {
      console.log(`rabbitmq not ready (${i}): ${(e as Error).message}`);
      await sleep(2000);
    }
  }
  if (!conn) throw new Error('rabbitmq unavailable');

  const channel = await conn.createChannel();
  await channel.assertExchange(exchangeName, 'topic', { durable: true });
  await channel.assertQueue(queueName, { durable: true });
  await channel.bindQueue(queueName, exchangeName, 'record.#');
  await channel.prefetch(100);

  console.log(`consumer listening on ${queueName}`);

  await channel.consume(queueName, async (msg) => {
    if (!msg) return;
    try {
      const body = JSON.parse(msg.content.toString()) as {
        event_id: string;
        record_id: number;
        version: number;
      };
      await pool.query(
        `INSERT INTO consumed_events (event_id, record_id, version)
         VALUES ($1, $2, $3)
         ON CONFLICT (event_id) DO UPDATE
           SET duplicate_hits = consumed_events.duplicate_hits + 1`,
        [body.event_id, body.record_id, body.version],
      );
      channel.ack(msg);
    } catch (e) {
      console.error(`consume failed: ${(e as Error).message}`);
      channel.nack(msg, false, true);
    }
  });
}

void main().catch((e) => {
  console.error(e);
  process.exit(1);
});
