import { Injectable, Logger } from '@nestjs/common';
import { EthereumNetworkType } from './interface';
import { ethers } from 'ethers';
import { ConfigService } from '@nestjs/config';
import R from 'ramda';

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
  constructor(private configService: ConfigService) {
    const network: ethers.providers.Networkish = this.configService.get('ethereum_network');

    const chainstackUrl: string = this.configService.get('chainstack_url')
    const chainStackProvider: ethers.providers.JsonRpcProvider = chainstackUrl
      ? new ethers.providers.JsonRpcProvider(chainstackUrl, network)
      : undefined;

    if (!chainStackProvider) {
      throw new Error(
        'Chainstack url is not defined',
      );
    }
        
    this.ether = chainStackProvider;
  }

  async getContractsInBlock(allAddress: string[], blockNum: number) {
    const currentTimestamp = new Date().getTime() / 1000;
    const provider = this.ether;
    const uniqueAddress = R.uniq(allAddress);
    const result = {};
    let processedNum = 0;
    for (const address of uniqueAddress) {
      if (address === '0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB') {
        result[address] = 'CryptoPunks';
        continue;
      }
      try {
        const contract = new ethers.Contract(address, ERC165ABI, provider);
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
  }

  async getLogsInBlock(blockNum: number) {
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
  } catch (error) {
    return undefined;
  }
  return undefined;
}
