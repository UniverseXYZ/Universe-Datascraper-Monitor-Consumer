export interface CreateNFTTokenDto {
  blockNumber?: number;
  contractAddress: string;
  tokenId: string;
  tokenType: string;
  firstOwner?: string;
  //for erc1155
  fromAddress?: string;
  toAddress?: string;
  value?: number;
  transactionHash?: string;
}
