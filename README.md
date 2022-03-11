# Universe Datascraper Monitor Consumer

## Description

This consumer is to fetch NFT contract addresses by a given block number from block producer. It scans all contract addresses in the block that are compatible with ERC721 or ERC1155 token. Then the contract addresses as new collections are stored in database for the next processing step.

It then analyze all possible tokens and transfers based on the collections retrieved from above.

Also it will analyze owners for both 721 and 1155. For 1155 it will generate a owner task for the owner flow to handle and for 721 it just directly write to owner database.

## Requirements:

- NodeJS version 14+
- NPM

## Required External Service

- AWS SQS
- Infura
- MongoDB

## Primary Third Party Libraries

- NestJS
- Mongoose (MongoDB)
- bbc/sqs-producer (Only applicable for producers)
- bbc/sqs-consumer (Only applicable for consumers)

## DataFlow

### Input Data

The block producer sends the messages that contain block numbers to this consumer.

### Data Analysis and Storage

This consumer scans the given block and extracts the contract addresses that are ERC721 or ERC1155 compliant.

### Output

After fetching and analysing the data from blockchain, it stores the NFT collections that container contract addresses and token types to database.

## MongoDB Collection Usage

This consumer leverage the following data collection in [schema](https://github.com/plugblockchain/Universe-Datascraper-Schema)

- NFT Block Task: To track the message processing status.
- NFT Collections: To store the extracted NFT collections (contract addresses).
