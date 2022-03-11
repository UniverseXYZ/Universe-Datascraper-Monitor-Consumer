import { Module } from '@nestjs/common';
import { EtherscanService } from './etherscan.service';

@Module({
  providers: [EtherscanService],
})
export class EtherscanModule {}
