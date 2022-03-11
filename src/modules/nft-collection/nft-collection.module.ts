import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { NFTCollectionService } from './nft-collection.service';
import {
  NFTCollection,
  NFTCollectionSchema,
} from './schemas/nft-collection.shema';

@Module({
  providers: [NFTCollectionService],
  exports: [NFTCollectionService],
  imports: [
    MongooseModule.forFeature([
      { name: NFTCollection.name, schema: NFTCollectionSchema },
    ]),
  ],
})
export class NFTCollectionModule {}
