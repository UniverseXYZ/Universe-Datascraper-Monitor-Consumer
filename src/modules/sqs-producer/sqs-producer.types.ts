import { SQS } from 'aws-sdk';

export interface Message<T = any> {
  id: string;
  body: T;
  groupId?: string;
  deduplicationId?: string;
  delaySeconds?: number;
  messageAttributes?: SQS.MessageBodyAttributeMap;
}

export interface SqsProducerHandler {
  sendMessage<T = any>(payload: Message<T> | Message<T>[]): void;
}
