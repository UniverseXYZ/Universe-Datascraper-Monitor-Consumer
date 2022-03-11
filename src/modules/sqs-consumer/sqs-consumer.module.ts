import { Module } from '@nestjs/common';
import { DalNFTTokenOwnerModule } from '../Dal/dal-nft-token-owner/dal-nft-token-owner.module';
import { DalNFTTokensModule } from '../Dal/dal-nft-token/dal-nft-token.module';
import { DalNFTTransferHistoryModule } from '../Dal/dal-nft-transfer-history/dal-nft-transfer-history.module';
import { EthereumModule } from '../ethereum/ethereum.module';
import { NFTBlockMonitorTaskModule } from '../nft-block-monitor-task/nft-block-monitor-task.module';
import { NFTCollectionModule } from '../nft-collection/nft-collection.module';
import { NFTTokenOwnersTaskModule } from '../nft-token-owners-task/nft-token-owners-task.module';
import { SqsConsumerService } from './sqs-consumer.service';

@Module({
  imports: [
    EthereumModule,
    NFTBlockMonitorTaskModule,
    NFTCollectionModule,
    DalNFTTransferHistoryModule,
    DalNFTTokensModule,
    NFTTokenOwnersTaskModule,
    DalNFTTokenOwnerModule,
  ],
  providers: [SqsConsumerService],
  exports: [SqsConsumerService],
})
export class SqsConsumerModule {}
