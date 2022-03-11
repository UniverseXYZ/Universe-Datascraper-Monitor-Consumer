import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { DalNFTTokenOwnerService } from './dal-nft-token-owner.service';
import {
  NFTTokenOwner,
  NFTTokenOwnerSchema,
} from './schemas/nft-token-owner.schema';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: NFTTokenOwner.name, schema: NFTTokenOwnerSchema },
    ]),
  ],
  providers: [DalNFTTokenOwnerService],
  exports: [DalNFTTokenOwnerService],
})
export class DalNFTTokenOwnerModule {}
