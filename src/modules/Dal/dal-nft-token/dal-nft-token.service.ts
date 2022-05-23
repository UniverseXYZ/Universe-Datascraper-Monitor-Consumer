import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { CreateNFTTokenDto } from './dto/create-nft-token.dto';
import { NFTToken, NFTTokensDocument } from './schemas/nft-token.schema';

@Injectable()
export class DalNFTTokensService {
  private readonly logger = new Logger(DalNFTTokensService.name);
  constructor(
    @InjectModel(NFTToken.name)
    private readonly nfttokensModel: Model<NFTTokensDocument>,
  ) {}

  async upsertTokens(tokens: CreateNFTTokenDto[], batchSize: number): Promise<void> {
    for (let i = 0; i < tokens.length; i += batchSize) {
        const tokensBatch = tokens.slice(i, i + batchSize);
  
        await this.nfttokensModel.bulkWrite(
          tokensBatch.map((x) => {
            const { contractAddress, tokenId, ...rest } = x;
            return {
              updateOne: {
                filter: { contractAddress: contractAddress, tokenId: tokenId },
                update: { ...rest },
                upsert: true,
              },
            };
          }),
          { ordered: false },
      );

      this.logger.log(`Batch ${i / batchSize + 1} completed`);
    }
  }

  // Deprecated (not used currently)
  //ERC721 is non fungible token which only has one tokenId
  async upsertERC721NFTTokens(tokens: CreateNFTTokenDto[]): Promise<void> {
    await this.nfttokensModel.bulkWrite(
      tokens.map((x) => ({
        updateOne: {
          filter: { contractAddress: x.contractAddress, tokenId: x.tokenId },
          update: {
            contractAddress: x.contractAddress,
            tokenId: x.tokenId,
            blockNumber: x.blockNumber,
            tokenType: x.tokenType,
            firstOwner: x.firstOwner,
          },
          upsert: true,
        },
      })),
      { ordered: false },
    );
  }

  // Deprecated (not used currently)
  //CryptoPunks is non fungible token which only has one tokenId
  async upsertCryptoPunksNFTTokens(tokens: CreateNFTTokenDto[]): Promise<void> {
    await this.nfttokensModel.bulkWrite(
      tokens.map((x) => {
        const { contractAddress, tokenId, ...rest } = x;
        return {
          updateOne: {
            filter: { contractAddress: contractAddress, tokenId: tokenId },
            update: { ...rest },
            upsert: true,
          },
        };
      }),
      { ordered: false },
    );
  }

// Deprecated (not used currently)
  async getExistingTokensByContractAddressAndTokenId(
    tokens: CreateNFTTokenDto[],
  ): Promise<NFTToken[]> {
    if (tokens?.length === 0) {
      return [];
    }
    //build query
    const query = {
      $or: tokens.map((x) => ({
        contractAddress: x.contractAddress,
        tokenId: x.tokenId,
      })),
    };
    //query all the tokens that have the same contract address and tokenId
    const existingTokens = await this.nfttokensModel.find(query);
    return existingTokens;
  }

  // Deprecated (not used currently)
  async insertTokens(toBeInsertedTokens: CreateNFTTokenDto[]) {
    await this.nfttokensModel.insertMany(toBeInsertedTokens, {
      ordered: false,
    });
  }

  // Deprecated (not used currently)
  async updateTokens(toBeUpdatedTokens: CreateNFTTokenDto[]) {
    await this.nfttokensModel.bulkWrite(
      toBeUpdatedTokens.map((x) => ({
        updateOne: {
          filter: { contractAddress: x.contractAddress, tokenId: x.tokenId },
          update: { $set: { ...x } },
          upsert: false,
        },
      })),
      { ordered: false },
    );
  }
}
