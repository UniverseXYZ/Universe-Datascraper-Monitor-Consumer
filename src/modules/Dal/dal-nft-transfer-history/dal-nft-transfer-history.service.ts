import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { CreateNFTTransferHistoryDto } from './dto/create-nft-transfer-history.dto';
import {
  NFTTransferHistory,
  NFTTransferHistoryDocument,
} from './schemas/nft-transfer-history.schema';

@Injectable()
export class DalNFTTransferHistoryService {
  private readonly logger = new Logger(DalNFTTransferHistoryService.name);
  constructor(
    @InjectModel(NFTTransferHistory.name)
    private readonly nftTransferHistoryModel: Model<NFTTransferHistoryDocument>,
  ) {}

  async createNFTTransferHistory(
    transferHistory: CreateNFTTransferHistoryDto,
  ): Promise<void> {
    await this.nftTransferHistoryModel.create(transferHistory);
  }

  async createERC721NFTTransferHistoryBatch(
    transferHistory: CreateNFTTransferHistoryDto[],
  ): Promise<void> {
    await this.nftTransferHistoryModel.bulkWrite(
      transferHistory.map((x) => ({
        updateOne: {
          filter: {
            tokenId: x.tokenId,
            contractAddress: x.contractAddress,
            hash: x.hash,
            logIndex: x.logIndex,
          },
          update: { $set: x },
          upsert: true,
        },
      })),
      { ordered: false },
    );
  }

  async createCryptoPunksNFTTransferHistoryBatch(
    transferHistory: CreateNFTTransferHistoryDto[],
  ): Promise<void> {
    await this.nftTransferHistoryModel.bulkWrite(
      transferHistory.map((x) => ({
        updateOne: {
          filter: {
            tokenId: x.tokenId,
            contractAddress: x.contractAddress,
            hash: x.hash,
            logIndex: x.logIndex,
          },
          update: { $set: x },
          upsert: true,
        },
      })),
      { ordered: false },
    );
  }

  async createERC1155NFTTransferHistoryBatch(
    transferHistory: CreateNFTTransferHistoryDto[],
  ): Promise<void> {
    await this.nftTransferHistoryModel.bulkWrite(
      transferHistory.map((x) => {
        const { hash, ...rest } = x;
        return {
          updateOne: {
            filter: {
              contractAddress: x.contractAddress,
              hash: hash,
              tokenId: x.tokenId,
              logIndex: x.logIndex,
            },
            update: { $set: { ...rest } },
            upsert: true,
          },
        };
      }),
      { ordered: false },
    );
  }
}
