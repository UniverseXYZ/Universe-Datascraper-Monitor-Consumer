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

  async updateERC721NFTTokenOwners(owners: CreateNFTTokenOwnerDto[]) {
    this.logger.log(`Update ${owners.length} token owners`);

    await this.nftTokenOwnerModel.bulkWrite(
      owners.map((x) => ({
        updateOne: {
          filter: { contractAddress: x.contractAddress, tokenId: x.tokenId },
          update: {
            ...x,
          },
        },
      })),
      { ordered: false },
    );
  }

  async createERC721NFTTokenOwners(owners: CreateNFTTokenOwnerDto[]) {
    this.logger.log(`Create ${owners.length} token owners`);
    await this.nftTokenOwnerModel.insertMany(owners);
  }

  async getERC721NFTTokenOwners(
    conditions: { contractAddress: string; tokenId: string }[],
  ): Promise<NFTTokenOwner[]> {
    const query = { $or: conditions };
    const owners = await this.nftTokenOwnerModel.find(query);
    return owners;
  }
}