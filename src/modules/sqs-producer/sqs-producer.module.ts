import { Module } from '@nestjs/common';
import { SqsProducerService } from './sqs-producer.service';

@Module({
  providers: [SqsProducerService],
  exports: [SqsProducerService],
})
export class SqsProducerModule {}
