import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { CreateNFTTokenOwnerDto } from './dto/create-nft-token-owner.dto';
import {
  NFTTokenOwner,
  NFTTokenOwnerDocument,
} from './schemas/nft-token-owner.schema';

@Injectable()
export class DalNFTTokenOwnerService {
  private readonly logger = new Logger(DalNFTTokenOwnerService.name);
  constructor(
    @InjectModel(NFTTokenOwner.name)
    private readonly nftTokenOwnerModel: Model<NFTTokenOwnerDocument>,
  ) {}

  async updateERC721NFTTokenOwners(owners: CreateNFTTokenOwnerDto[], batchSize: number) {
    for (let i = 0; i < owners.length; i += batchSize) {
      const ownersBatch = owners.slice(i, i + batchSize);

      await this.nftTokenOwnerModel.bulkWrite(
        ownersBatch.map((x) => ({
          updateOne: {
            filter: {
              contractAddress: x.contractAddress,
              tokenId: x.tokenId,
              $or: [
                { blockNum: { $lt: x.blockNum } },
                { blockNum: x.blockNum, logIndex: { $lt: x.logIndex } },
              ],
            },
            update: {
              ...x,
            },
          },
        })),
        { ordered: false },
      );
  
      this.logger.log(`Batch ${i+1} completed`);
    }
  }

  async upsertERC721NFTTokenOwners(owners: CreateNFTTokenOwnerDto[], batchSize: number) {
    for (let i = 0; i < owners.length; i += batchSize) {
      const ownersBatch = owners.slice(i, i + batchSize);

      await this.nftTokenOwnerModel.bulkWrite(
        ownersBatch.map((x) => ({
          updateOne: {
            filter: {
              contractAddress: x.contractAddress,
              tokenId: x.tokenId,
              $or: [
                { blockNum: { $lt: x.blockNum } },
                { blockNum: x.blockNum, logIndex: { $lt: x.logIndex } },
              ],
            },
            update: {
              ...x,
            },
            upsert: true,
          },
        })),
        { ordered: false },
      );
    
      this.logger.log(`Batch ${i+1} completed`);
    }

  }

  async getERC721NFTTokenOwners(
    conditions: { contractAddress: string; tokenId: string }[],
  ): Promise<NFTTokenOwner[]> {
    const query = { $or: conditions };
    const owners = await this.nftTokenOwnerModel.find(query);
    return owners;
  }
}
