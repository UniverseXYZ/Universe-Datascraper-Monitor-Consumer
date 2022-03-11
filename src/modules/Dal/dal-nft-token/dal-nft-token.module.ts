import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { DalNFTTokensService } from './dal-nft-token.service';
import { NFTToken, NFTTokensSchema } from './schemas/nft-token.schema';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: NFTToken.name, schema: NFTTokensSchema },
    ]),
  ],
  providers: [DalNFTTokensService],
  exports: [DalNFTTokensService],
})
export class DalNFTTokensModule {}
