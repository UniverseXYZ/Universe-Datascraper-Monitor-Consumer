import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { CreateOwnerTaskDto } from './dto/create-owner-task.dto';
import {
  NFTTokenOwnersTask,
  NFTTokenOwnersTaskDocument,
} from './schemas/nft-token-owners-task.schema';

@Injectable()
export class NFTTokenOwnersTaskService {
  private readonly logger = new Logger(NFTTokenOwnersTaskService.name);
  constructor(
    @InjectModel(NFTTokenOwnersTask.name)
    private readonly nftTokenOwnersTaskModel: Model<NFTTokenOwnersTaskDocument>,
  ) {}

  public async findUnprocessed(amount: number) {
    return await this.nftTokenOwnersTaskModel
      .find(
        {
          isProcessing: { $in: [null, false] },
        },
        {},
        { sort: { priority: 1, createdAt: -1 } },
      )
      .limit(amount || 1);
  }

  async setTaskInProcessing(contractAddress: string, tokenId: string) {
    await this.nftTokenOwnersTaskModel.findOneAndUpdate(
      { contractAddress, tokenId },
      { isProcessing: true, sentAt: new Date() },
    );
  }

  async upsertTasks(tasks: CreateOwnerTaskDto[], batchSize: number): Promise<void> {
    for (let i = 0; i < tasks.length; i += batchSize) {
      const tasksBatch = tasks.slice(i, i + batchSize);

      await this.nftTokenOwnersTaskModel.insertMany(
        tasksBatch.map((x) => ({
          contractAddress: x.contractAddress,
          tokenId: x.tokenId,
          priority: 10,
          isProcessing: false,
          tokenType: x.tokenType,
          taskId: x.taskId,
        })),
        { ordered: false },
      );

      this.logger.log(`Batch ${i / batchSize + 1} completed`);
    }
  }
}
