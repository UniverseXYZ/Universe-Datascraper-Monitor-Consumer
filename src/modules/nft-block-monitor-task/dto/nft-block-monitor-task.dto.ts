import { MessageStatus } from 'datascraper-schema';

export interface CreateNFTBlockMonitorTaskDto {
  messageId: string;
  blockNum: number;
  status: MessageStatus;
  errorMessage?: string;
}
