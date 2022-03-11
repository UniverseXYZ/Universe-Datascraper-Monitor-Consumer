import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { EthereumModule } from '../ethereum/ethereum.module';
import { EthHealthIndicator } from './eth-health.service';

@Module({
  imports: [EthereumModule, TerminusModule],
  providers: [EthHealthIndicator],
  exports: [EthHealthIndicator],
})
export class EthHealthModule {}
