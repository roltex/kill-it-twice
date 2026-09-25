import 'reflect-metadata';
import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ApiController } from './api.controller';
import { loadConfig } from './config';
import { DbService } from './db.service';
import { ElasticsearchSink } from './elasticsearch.sink';
import { PipelineService } from './pipeline.service';
import { RabbitSink } from './rabbit.sink';
import { RedisService } from './redis.service';

@Module({
  controllers: [ApiController],
  providers: [DbService, RedisService, ElasticsearchSink, RabbitSink, PipelineService],
})
class AppModule {}

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors();
  const { port } = loadConfig();
  await app.listen(port, '0.0.0.0');
}

void bootstrap();
