import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { NFTTokenOwnersTaskService } from './nft-token-owners-task.service';
import {
  NFTTokenOwnersTask,
  NFTTokenOwnersTaskSchema,
} from './schemas/nft-token-owners-task.schema';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: NFTTokenOwnersTask.name, schema: NFTTokenOwnersTaskSchema },
    ]),
  ],
  providers: [NFTTokenOwnersTaskService],
  exports: [NFTTokenOwnersTaskService],
})
export class NFTTokenOwnersTaskModule {}
