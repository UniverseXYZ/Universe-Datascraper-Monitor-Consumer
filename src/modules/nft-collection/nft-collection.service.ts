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

  public async insertIfNotThere(collections: CreateNFTCollectionDto[]) {
    await this.nftCollectionModel.bulkWrite(
      collections.map((collection) => ({
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
  }
}
