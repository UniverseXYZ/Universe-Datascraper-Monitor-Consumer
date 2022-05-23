import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { CreateNFTCollectionDto } from './dto/create-nft-collection.dto';
import {
  NFTCollection,
  NFTCollectionDocument,
} from './schemas/nft-collection.shema';

@Injectable()
export class NFTCollectionService {
  private readonly logger = new Logger(NFTCollectionService.name);

  constructor(
    @InjectModel(NFTCollection.name)
    private readonly nftCollectionModel: Model<NFTCollectionDocument>,
  ) {}

  public async findUnprocessedOne() {
    return await this.nftCollectionModel.findOne({
      sentAt: null,
      firstCheckAt: null,
    });
  }

  public async markAsChecked(contractAddress: string) {
    await this.nftCollectionModel.updateOne(
      {
        contractAddress,
      },
      {
        firstCheckAt: new Date(),
      },
    );
  }

  public async markAsProcessed(contractAddress: string) {
    await this.nftCollectionModel.updateOne(
      {
        contractAddress,
      },
      {
        sentAt: new Date(),
      },
    );
  }

  public async insertIfNotThere(collections: CreateNFTCollectionDto[], batchSize: number) {
    for (let i = 0; i < collections.length; i += batchSize) {
      const collectionsBatch = collections.slice(i, i + batchSize);

      await this.nftCollectionModel.bulkWrite(
        collectionsBatch.map((collection) => ({
          updateOne: {
            filter: {
              contractAddress: collection.contractAddress,
            },
            update: {
              $set: {
                contractAddress: collection.contractAddress,
                tokenType: collection.tokenType,
              },
            },
            upsert: true,
          },
        })),
        { ordered: false },
      );
          
      this.logger.log(`Batch ${i / batchSize + 1} completed`);
    }
  }
}
