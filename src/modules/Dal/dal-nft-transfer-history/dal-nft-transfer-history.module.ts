import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { DalNFTTransferHistoryService } from './dal-nft-transfer-history.service';
import {
  NFTTransferHistory,
  NFTTransferHistorySchema,
} from './schemas/nft-transfer-history.schema';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: NFTTransferHistory.name, schema: NFTTransferHistorySchema },
    ]),
  ],
  providers: [DalNFTTransferHistoryService],
  exports: [DalNFTTransferHistoryService],
})
export class DalNFTTransferHistoryModule {}
