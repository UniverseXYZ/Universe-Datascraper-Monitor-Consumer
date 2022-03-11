import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios from 'axios';
import { ethers } from 'ethers';

@Injectable()
export class EtherscanService {
  constructor(protected readonly configService: ConfigService) {}

  public async getContractAbi(address: string) {
    const isValidAddress = ethers.utils.isAddress(address);

    if (!isValidAddress) {
      return { success: false, message: 'Invalid address' };
    }

    const etherscan_api_key = this.configService.get('etherscan_api_key');

    if (!etherscan_api_key) {
      return { success: false, message: 'Etherscan API key is not defined' };
    }

    const url = `https://api.etherscan.io/api?module=contract&action=getabi&address=${address}&apikey=${etherscan_api_key}`;
    const response = await axios.get(url);
    return { success: true, abi: response.data.result };
  }
}
