import { Injectable, Logger } from '@nestjs/common';
import { ethers } from 'ethers';
import { ConfigService } from '@nestjs/config';
import R from 'ramda';
import { Utils } from 'src/utils';
import { Cron, CronExpression } from '@nestjs/schedule';

const ERC165ABI = [
  {
    constant: true,
    inputs: [{ internalType: 'bytes4', name: '', type: 'bytes4' }],
    name: 'supportsInterface',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
    payable: false,
    stateMutability: 'view',
    type: 'function',
  },
];
@Injectable()
export class EthereumService {
  public ether: ethers.providers.StaticJsonRpcProvider;
  
  private definedProviders: ethers.providers.StaticJsonRpcProvider[];
  private allProviders: ethers.providers.StaticJsonRpcProvider[];
  private providerIndex: number = -1;

  private readonly logger = new Logger(EthereumService.name);

  constructor(private configService: ConfigService) {
    const network: ethers.providers.Networkish = this.configService.get('ethereum_network');
    
    const infuraSecret: string = this.configService.get('infura.project_secret');
    const infuraId: string = this.configService.get('infura.project_id');

    const infuraProvider: ethers.providers.InfuraProvider = infuraId && infuraSecret ? 
      new ethers.providers.InfuraProvider(network, {projectId: infuraId, projectSecret: infuraSecret}) 
      : undefined;

    const alchemyToken: string = this.configService.get('alchemy_token');
    const alchemyProvider: ethers.providers.AlchemyProvider = alchemyToken
      ? new ethers.providers.AlchemyProvider(network, alchemyToken)
      : undefined;

    const chainstackUrl: string = this.configService.get('chainstack_url');
    const chainStackProvider: ethers.providers.StaticJsonRpcProvider = chainstackUrl
      ? new ethers.providers.StaticJsonRpcProvider(chainstackUrl, network)
      : undefined;

    const quicknodeUrl: string = this.configService.get('quicknode_url');
    const quicknodeProvider: ethers.providers.StaticJsonRpcProvider = quicknodeUrl
      ? new ethers.providers.StaticJsonRpcProvider(quicknodeUrl, network)
      : undefined;

    if (
      !infuraProvider &&
      !alchemyProvider &&
      !chainStackProvider &&
      !quicknodeProvider
    ) {
      throw new Error(
        'Quorum or Infura project id or secret or alchemy token or chainstack url is not defined',
      );
    }

    const allProviders: ethers.providers.StaticJsonRpcProvider[] = [
      infuraProvider,
      alchemyProvider,
      quicknodeProvider,
      chainStackProvider,
    ];

    const definedProviders: ethers.providers.StaticJsonRpcProvider[] =
      allProviders.filter((x) => x !== undefined);

    this.definedProviders = definedProviders;
    this.allProviders = allProviders;

    this.connectToProvider();
  }

  /**
   * Try to start using the first provider
   */
  @Cron(CronExpression.EVERY_DAY_AT_MIDNIGHT)
  public async resetProviderIndex() {
    this.providerIndex = -1;
    await this.connectToProvider();
  }
  
  /**
   * Rotate through every defined provider and try to connect to one of them. If none are availabe service will stop execution
   * @param callback Function to execute if successfully connected to provider
   * @returns result of callback
   */
  public async connectToProvider(callback?: any) {
    this.ether = null;
    this.logger.warn("Initiating provider rotation logic! Beep boop...");

    // Start from the current index provider and go through all of them
    for (let i = this.providerIndex + 1; i < this.definedProviders.length + this.providerIndex + 1; i++) {
      try {
        const provider = this.definedProviders[i % this.definedProviders.length];
        const currentBlockNumber = await provider.getBlockNumber();
        if (isNaN(currentBlockNumber)) {
          continue;
        }

        this.ether = provider;
        this.providerIndex = i % this.definedProviders.length;

        this.logger.log(`Connected to ${this.getProviderName()} provider. Block number: ${currentBlockNumber}. Configured ${this.definedProviders.length} out of ${this.allProviders.length} Providers`)
        break;

      } catch(err) {
        this.logger.warn(`${this.getProviderName()} isn't available... Trying to connect to another one.`)
      }
    }

    if (!this.ether) {
      this.logger.warn("Couldn't find working provider. Stopping execution of microservice.");
      Utils.shutdown();
    }

    if (callback) {
      return callback();
    }
  }

  public async getBlockNum() {
    try {
      return this.ether.getBlockNumber();
    } catch(err) {
      this.logger.warn("Failed to get block number.")
      return this.connectToProvider(() => this.getBlockNum());
    }
  }

  private getProviderName() {
    switch (this.providerIndex) {
      case 0:
        return "Infura";
      case 1:
        return "Alchemy";
      case 2:
        return "Quicknode";
        case 3:
        return "Chainstack";
      default:
        return "Unknown provider";
    }
  }

  async getContractsInBlock(allAddress: string[], blockNum: number) {
    try {
      const currentTimestamp = new Date().getTime() / 1000;
      const uniqueAddress = R.uniq(allAddress);
      const result = {};
      let processedNum = 0;
      for (const address of uniqueAddress) {
        if (address === '0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB') {
          result[address] = 'CryptoPunks';
          continue;
        }
        try {
          const contract = new ethers.Contract(address, ERC165ABI, this.ether);
          const type = await getERCtype(contract);
          processedNum++;
          if (type) {
            result[address] = type;
          }
          continue;
        } catch (error) {
          continue;
        }
      }
      const endTimestamp = new Date().getTime() / 1000;
      this.logger.log(
        `[${blockNum}] total processing time spent for abi verified ${processedNum}/${
          uniqueAddress.length
        } collections with ${endTimestamp - currentTimestamp} seconds`,
      );
      return result;
    } catch(err) {
      this.logger.log("Failed to get contracts in block.");
      return this.connectToProvider(() => this.getContractsInBlock(allAddress, blockNum));
    }
  }

  async getLogsInBlock(blockNum: number) {
    try {
      // const ERC721Transfer = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
      // const CryptoPunksPunkTransfer = "0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8";
      // const CryptoPunksPunkBought= "0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3";
      // const ERC1155TransferSingle = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";
      // const ERC1155TransferBatch = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb";
      const topics = [
        [
          '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
          '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8',
          '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3',
          '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
          '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb',
        ],
      ];
      const filter = {
        fromBlock: blockNum,
        toBlock: blockNum,
        topics: topics,
      };
      const result = await this.ether.getLogs(filter);
      return result;
    } catch(err) {
      this.logger.log("Failed to get logs in blocks.")
      return this.connectToProvider(() => this.getLogsInBlock(blockNum));
    }
  }
}

async function getERCtype(contract: any) {
  try {
    const is721 = await contract.supportsInterface('0x80ac58cd');
    if (is721) {
      return 'ERC721';
    }
    const is1155 = await contract.supportsInterface('0xd9b67a26');
    if (is1155) {
      return 'ERC1155';
    }

    // Code should go in this case only if contract doesn't properly implement ERC721A's supportsInterface
    return "ERC721"
  } catch (error) {
    return this.connectToProvider(() => this.getERCtype(contract));
  }
  return undefined;
}
