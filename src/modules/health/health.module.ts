import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { DbHealthModule } from '../db-health/db-health.module';
import { DbHealthService } from '../db-health/db-health.service';
import { EthHealthModule } from '../eth-health/eth-health.module';
import { EthHealthIndicator } from '../eth-health/eth-health.service';
import { EthereumModule } from '../ethereum/ethereum.module';
import { HealthController } from './health.controller';

@Module({
  imports: [TerminusModule, EthHealthModule, EthereumModule, DbHealthModule],
  providers: [EthHealthIndicator, DbHealthService],
  controllers: [HealthController],
})
export class HealthModule {}
