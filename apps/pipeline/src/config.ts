export interface AppConfig {
  port: number;
  databaseUrl: string;
  redisUrl: string;
  elasticsearchUrl: string;
  rabbitmqUrl: string;
  batchSize: number;
  maxInFlight: number;
  backoffStartMs: number;
  backoffCapMs: number;
  queueName: string;
  exchangeName: string;
  indexName: string;
}

export function loadConfig(): AppConfig {
  return {
    port: Number(process.env.PORT ?? 3000),
    databaseUrl: process.env.DATABASE_URL ?? 'postgres://optio:optio@localhost:5432/optio',
    redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6379',
    elasticsearchUrl: process.env.ELASTICSEARCH_URL ?? 'http://localhost:9200',
    rabbitmqUrl: process.env.RABBITMQ_URL ?? 'amqp://optio:optio@localhost:5672',
    batchSize: Number(process.env.BATCH_SIZE ?? 500),
    maxInFlight: Number(process.env.MAX_IN_FLIGHT ?? 2),
    backoffStartMs: Number(process.env.BACKOFF_START_MS ?? 1000),
    backoffCapMs: Number(process.env.BACKOFF_CAP_MS ?? 15000),
    queueName: process.env.QUEUE_NAME ?? 'optio.changes',
    exchangeName: process.env.EXCHANGE_NAME ?? 'optio.changes',
    indexName: process.env.INDEX_NAME ?? 'records',
  };
}
