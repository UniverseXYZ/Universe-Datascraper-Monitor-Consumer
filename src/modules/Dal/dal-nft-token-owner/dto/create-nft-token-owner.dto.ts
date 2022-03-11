export interface CreateNFTTokenOwnerDto {
  transactionHash: string;
  contractAddress: string;
  tokenId: string;
  blockNum: number;
  logIndex: number;
  tokenType: string;
  address: string;
  value: string;
}
