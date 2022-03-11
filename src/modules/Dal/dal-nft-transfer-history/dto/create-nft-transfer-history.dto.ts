export interface CreateNFTTransferHistoryDto {
  contractAddress: string;
  blockNum: number;
  from: string;
  to: string;
  hash: string;
  logIndex: number;
  tokenId: string;
  value?: number;
  erc721TokenId?: string;
  erc1155Metadata?: any;
  cryptopunks?: any;
  asset?: string;
  category: string;
  timeLastUpdated?: string;
}
