import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { CreateNFTBlockMonitorTaskDto } from './dto/nft-block-monitor-task.dto';
import {
  NFTBlockMonitorTask,
  NFTBlockMonitorTaskDocument,
} from './schemas/nft-block-monitor-task.schema';

@Injectable()
export class NFTBlockMonitorTaskService {
  private readonly logger = new Logger(NFTBlockMonitorTaskService.name);
  private readonly CURRENT_MONITOR_BLOCK = 'CURRENT_MONITOR_BLOCK';

  constructor(
    @InjectModel(NFTBlockMonitorTask.name)
    private readonly nftBlockMonitorTaskModel: Model<NFTBlockMonitorTaskDocument>,
  ) {}

  async insertLatestOne(blockNum: number) {
    await this.nftBlockMonitorTaskModel.insertMany({
      messageId: this.CURRENT_MONITOR_BLOCK,
      blockNum,
      status: 'sent',
    });
  }

  async updateLatestOne(blockNum: number) {
    await this.nftBlockMonitorTaskModel.updateOne(
      { messageId: this.CURRENT_MONITOR_BLOCK },
      { blockNum },
    );
  }

  async getLatestOne() {
    return await this.nftBlockMonitorTaskModel.findOne({
      messageId: this.CURRENT_MONITOR_BLOCK,
    });
  }

  async updateNFTBlockMonitorTask(
    task: CreateNFTBlockMonitorTaskDto,
  ): Promise<void> {
    this.logger.log(`update task ${task.messageId} status (${task.status})`);
    await this.nftBlockMonitorTaskModel.updateOne(
      { messageId: task.messageId },
      { $set: { ...task } },
      { upsert: true },
    );
  }

  async removeNFTBlockMonitorTask(messageId: string) {
    this.logger.log(`remove task ${messageId}`);
    await this.nftBlockMonitorTaskModel.deleteOne({
      messageId,
    });
  }
}
