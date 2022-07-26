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
}
