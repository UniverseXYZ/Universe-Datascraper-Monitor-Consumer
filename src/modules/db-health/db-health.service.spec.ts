import { Test, TestingModule } from '@nestjs/testing';
import { DbHealthService } from './db-health.service';

describe('DbHealthService', () => {
  let service: DbHealthService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [DbHealthService],
    }).compile();

    service = module.get<DbHealthService>(DbHealthService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
