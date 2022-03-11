export interface CreateNFTCollectionDto {
  contractAddress: string;
  tokenType: 'ERC721' | 'ERC1155';
}
