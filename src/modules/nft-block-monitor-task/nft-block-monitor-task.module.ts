import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { NFTBlockMonitorTaskService } from './nft-block-monitor-task.service';
import {
  NFTBlockMonitorTask,
  NFTBlockMonitorTaskSchema,
} from './schemas/nft-block-monitor-task.schema';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: NFTBlockMonitorTask.name, schema: NFTBlockMonitorTaskSchema },
    ]),
  ],
  providers: [NFTBlockMonitorTaskService],
  exports: [NFTBlockMonitorTaskService],
})
export class NFTBlockMonitorTaskModule {}
