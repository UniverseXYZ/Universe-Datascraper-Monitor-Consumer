import { Injectable, OnModuleInit } from '@nestjs/common';
import { Producer } from 'sqs-producer';
import AWS from 'aws-sdk';
import { Message, SqsProducerHandler } from './sqs-producer.types';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class SqsProducerService implements OnModuleInit, SqsProducerHandler {
  public sqsProducer: Producer;

  constructor(private configService: ConfigService) {
    AWS.config.update({
      region: this.configService.get('aws.region'),
      accessKeyId: this.configService.get('aws.accessKeyId'),
      secretAccessKey: this.configService.get('aws.secretAccessKey'),
    });
  }

  public onModuleInit() {
    this.sqsProducer = Producer.create({
      queueUrl: this.configService.get('aws.queueUrl'),
      sqs: new AWS.SQS(),
    });
  }

  async sendMessage<T = any>(
    payload: Message<T> | Message<T>[],
  ): Promise<void> {
    const originalMessages = Array.isArray(payload) ? payload : [payload];
    const messages = originalMessages.map((message) => {
      let body = message.body;
      if (typeof body !== 'string') {
        body = JSON.stringify(body) as any;
      }

      return {
        ...message,
        body,
      };
    });

    await this.sqsProducer.send(messages as any[]);
  }
}
