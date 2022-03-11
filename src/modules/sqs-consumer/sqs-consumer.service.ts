import {
  Logger,
  Injectable,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { Consumer } from 'sqs-consumer';
import AWS from 'aws-sdk';
import {
  ERROR_EVENT_NAME,
  PROCESSING_ERROR_EVENT_NAME,
  ReceivedMessage,
  TIMEOUT_EVENT_NAME,
  MESSAGE_PROCESSED_EVENT_NAME,
} from './sqs-consumer.types';
import { ConfigService } from '@nestjs/config';
import { EthereumService } from '../ethereum/ethereum.service';
import https from 'https';
import { NFTCollectionService } from '../nft-collection/nft-collection.service';
import { DalNFTTransferHistoryService } from '../Dal/dal-nft-transfer-history/dal-nft-transfer-history.service';
import { DalNFTTokensService } from '../Dal/dal-nft-token/dal-nft-token.service';
import { ethers } from 'ethers';
import R from 'ramda';
import { v4 as uuidv4 } from 'uuid';
import { NFTTokenOwnersTaskService } from '../nft-token-owners-task/nft-token-owners-task.service';
import { NFTBlockMonitorTaskService } from '../nft-block-monitor-task/nft-block-monitor-task.service';
import { MessageStatus, NFTTokenOwner } from 'datascraper-schema';
import { DalNFTTokenOwnerService } from '../Dal/dal-nft-token-owner/dal-nft-token-owner.service';
import { TransferHistory } from '../ethereum/ethereum.types';

const abiCoder = new ethers.utils.AbiCoder();
const decodeAddress = (data: string) => {
  return abiCoder.decode(['address'], data)[0];
};

@Injectable()
export class SqsConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SqsConsumerService.name);
  public sqsConsumer: Consumer;
  public queue: AWS.SQS;

  constructor(
    private readonly configService: ConfigService,
    private readonly etherService: EthereumService,
    private readonly nftBlockMonitorTaskService: NFTBlockMonitorTaskService,
    private readonly nftCollectionService: NFTCollectionService,
    private readonly dalNFTTransferHistoryService: DalNFTTransferHistoryService,
    private readonly dalNFTTokensService: DalNFTTokensService,
    private readonly nftTokenOwnersTaskService: NFTTokenOwnersTaskService,
    private readonly nftTokenOwnerService: DalNFTTokenOwnerService,
  ) {
    const region = this.configService.get('aws.region');
    const accessKeyId = this.configService.get('aws.accessKeyId');
    const secretAccessKey = this.configService.get('aws.secretAccessKey');

    if (!region || !accessKeyId || !secretAccessKey) {
      throw new Error(
        'Initialize AWS queue failed, please check required variables',
      );
    }

    AWS.config.update({
      region,
      accessKeyId,
      secretAccessKey,
    });
  }

  public onModuleInit() {
    this.logger.log('onModuleInit');
    this.queue = new AWS.SQS({
      httpOptions: {
        agent: new https.Agent({
          keepAlive: true,
        }),
      },
    });
    this.sqsConsumer = Consumer.create({
      queueUrl: this.configService.get('aws.queueUrl'),
      sqs: this.queue,
      handleMessage: this.handleMessage.bind(this),
    });

    this.logger.log('Register events');
    //listen to events
    this.sqsConsumer.addListener(ERROR_EVENT_NAME, this.onError.bind(this));
    this.sqsConsumer.addListener(
      PROCESSING_ERROR_EVENT_NAME,
      this.onProcessingError.bind(this),
    );
    this.sqsConsumer.addListener(
      TIMEOUT_EVENT_NAME,
      this.onTimeoutError.bind(this),
    );
    this.sqsConsumer.addListener(
      MESSAGE_PROCESSED_EVENT_NAME,
      this.onMessageProcessed.bind(this),
    );

    this.logger.log('Consumer starts');
    this.sqsConsumer.start();
  }

  public onModuleDestroy() {
    this.logger.log('Consumer stops');
    this.sqsConsumer.stop();
  }

  async handleOwners(transferHistories: TransferHistory[]) {
    if (transferHistories.length === 0) return;

    this.logger.log('Start handling token owners');

    const latestHistory = this.getLatestHistory(transferHistories);

    const owners = await this.nftTokenOwnerService.getERC721NFTTokenOwners(
      latestHistory.map((x) => ({
        contractAddress: x.contractAddress,
        tokenId: x.tokenId,
      })),
    );

    const { toBeInsertedOwners, toBeUpdatedOwners } = this.calculateOwners(
      latestHistory,
      owners,
    );

    await this.nftTokenOwnerService.createERC721NFTTokenOwners(
      toBeInsertedOwners,
    );

    await this.nftTokenOwnerService.updateERC721NFTTokenOwners(
      toBeUpdatedOwners,
    );
  }

  private calculateOwners(
    latestHistory: TransferHistory[],
    owners: NFTTokenOwner[],
  ) {
    const toBeInsertedOwners = [];
    const toBeUpdatedOwners = [];

    for (const history of latestHistory) {
      const { tokenId, contractAddress, to, blockNum, logIndex, category } =
        history;

      const owner = owners.find(
        (x) => x.tokenId === tokenId && x.contractAddress === contractAddress,
      );

      const newOwner = {
        tokenId,
        contractAddress,
        address: to,
        blockNum,
        logIndex,
        tokenType: category,
        transactionHash: history.hash,
        value: '1',
      };

      if (!owner) {
        toBeInsertedOwners.push(newOwner);
        continue;
      }
      if (owner.blockNum > blockNum) continue;

      if (owner.blockNum === blockNum && owner.logIndex > logIndex) continue;

      toBeUpdatedOwners.push(newOwner);
    }

    return { toBeInsertedOwners, toBeUpdatedOwners };
  }

  private getLatestHistory(transferHistories: TransferHistory[]) {
    const groupedTransferHistories =
      this.groupTransferHistoryByTokenId(transferHistories);

    const latestTransferHistory = Object.keys(groupedTransferHistories).map(
      (tokenId) => {
        // sort descending
        const historiesWithTokenId = groupedTransferHistories[tokenId].sort(
          (a, b) => b.blockNum - a.blockNum,
        );

        return historiesWithTokenId[0];
      },
    );
    return latestTransferHistory;
  }

  private groupTransferHistoryByTokenId(transferHistories: TransferHistory[]) {
    const groupByTokenId = R.groupBy((history: TransferHistory) => {
      return history.tokenId;
    });

    const grouped = groupByTokenId(transferHistories);

    return grouped;
  }

  async handleMessage(message: AWS.SQS.Message) {
    this.logger.log(`Consumer handle message id:(${message.MessageId})`);
    const receivedMessage = JSON.parse(message.Body) as ReceivedMessage;

    const nftBlockTask = {
      messageId: message.MessageId,
      blockNum: receivedMessage.blockNum,
    };

    this.logger.log(`Set message id:(${message.MessageId}) as processing`);
    await this.nftBlockMonitorTaskService.updateNFTBlockMonitorTask({
      ...nftBlockTask,
      status: MessageStatus.processing,
    });
    const ERC721Transfer =
      '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
    const CryptoPunksPunkTransfer =
      '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8';
    const CryptoPunksPunkBought =
      '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3';
    const ERC1155TransferSingle =
      '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62';
    const ERC1155TransferBatch =
      '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb';
    const logInBlock = await this.etherService.getLogsInBlock(
      nftBlockTask.blockNum,
    );
    const allAddress = [];
    const cryptopunkBatch = [];
    const erc721Batch = [];
    const erc1155Batch = [];
    const tokens = [];
    const ownerTasks = [];
    this.logger.log(
      `Start handling ${logInBlock.length} logs in block: ${nftBlockTask.blockNum}`,
    );
    for (const x of logInBlock) {
      const { address, data, topics, blockNumber, transactionHash, logIndex } =
        x;
      switch (topics[0]) {
        case CryptoPunksPunkTransfer:
          const punkIndex = ethers.BigNumber.from(data).toString();
          cryptopunkBatch.push({
            contractAddress: address,
            blockNum: blockNumber,
            hash: transactionHash,
            logIndex: logIndex,
            from: decodeAddress(topics[1]),
            to: decodeAddress(topics[2]),
            tokenId: punkIndex,
            value: 1,
            cryptopunks: {
              punkIndex: punkIndex,
            },
            category: 'CryptoPunks',
          });
          allAddress.push({
            contractAddress: address,
            tokenType: 'CryptoPunks',
          });
          tokens.push({
            contractAddress: address,
            tokenType: 'CryptoPunks',
            tokenId: punkIndex,
            source: 'MONITOR',
          });
          break;
        case CryptoPunksPunkBought:
          const _punkIndex = ethers.BigNumber.from(topics[1]).toString();
          cryptopunkBatch.push({
            contractAddress: address,
            blockNum: blockNumber,
            hash: transactionHash,
            logIndex: logIndex,
            from: decodeAddress(topics[2]),
            to: decodeAddress(topics[3]),
            tokenId: _punkIndex,
            value: 1,
            cryptopunks: {
              punkIndex: _punkIndex,
            },
            category: 'CryptoPunks',
          });
          allAddress.push({
            contractAddress: address,
            tokenType: 'CryptoPunks',
          });
          tokens.push({
            contractAddress: address,
            tokenType: 'CryptoPunks',
            tokenId: _punkIndex,
            source: 'MONITOR',
          });
          break;
        case ERC721Transfer:
          // erc20 and erc721 topic is the same, thus we can differentiate them by checking the length of topics
          if (topics.length < 4) {
            break;
          }
          const erc721TokenId = ethers.BigNumber.from(topics[3]).toString();
          erc721Batch.push({
            contractAddress: address,
            blockNum: blockNumber,
            hash: transactionHash,
            logIndex: logIndex,
            from: decodeAddress(topics[1]),
            to: decodeAddress(topics[2]),
            tokenId: erc721TokenId,
            category: 'ERC721',
          });
          allAddress.push({
            contractAddress: address,
            tokenType: 'ERC721',
          });
          tokens.push({
            contractAddress: address,
            tokenType: 'ERC721',
            tokenId: erc721TokenId,
            source: 'MONITOR',
          });
          break;
        case ERC1155TransferSingle:
          const [_id, _value] = abiCoder.decode(['uint256', 'uint256'], data);
          erc1155Batch.push({
            contractAddress: address,
            blockNum: blockNumber,
            hash: transactionHash,
            logIndex: logIndex,
            from: decodeAddress(topics[2]),
            to: decodeAddress(topics[3]),
            tokenId: _id.toString(),
            erc1155Metadata: {
              tokenId: _id.toString(),
              value: ethers.BigNumber.from(_value).toString(),
            },
            category: 'ERC1155',
          });
          allAddress.push({
            contractAddress: address,
            tokenType: 'ERC1155',
          });
          tokens.push({
            contractAddress: address,
            tokenType: 'ERC1155',
            tokenId: _id.toString(),
            value: ethers.BigNumber.from(_value).toString(),
            transactionHash: transactionHash,
            source: 'MONITOR',
          });
          ownerTasks.push({
            contractAddress: address,
            tokenType: 'ERC1155',
            tokenId: _id.toString(),
            taskId: uuidv4(),
          });
          break;
        case ERC1155TransferBatch:
          const [tokenIds, values] = abiCoder.decode(
            ['uint256[]', 'uint256[]'],
            data,
          );
          tokenIds.forEach((tokenId, index) => {
            erc1155Batch.push({
              contractAddress: address,
              blockNum: blockNumber,
              hash: transactionHash,
              logIndex: logIndex,
              from: decodeAddress(topics[2]),
              to: decodeAddress(topics[3]),
              tokenId: tokenId.toString(),
              erc1155Metadata: {
                tokenId: tokenId.toString(),
                value: ethers.BigNumber.from(values[index]).toString(),
              },
              category: 'ERC1155',
            });
            tokens.push({
              contractAddress: address,
              tokenType: 'ERC1155',
              tokenId: tokenId.toString(),
              value: ethers.BigNumber.from(values[index]).toString(),
              transactionHash: transactionHash,
              source: 'MONITOR',
            });
            ownerTasks.push({
              contractAddress: address,
              tokenType: 'ERC1155',
              tokenId: tokenId.toString(),
              taskId: uuidv4(),
            });
          });
          allAddress.push({
            contractAddress: address,
            tokenType: 'ERC1155',
          });
          break;
        default:
          break;
      }
    }
    this.logger.log('end handling logs in block: ' + nftBlockTask.blockNum);
    await this.dalNFTTransferHistoryService.createERC721NFTTransferHistoryBatch(
      erc721Batch,
    );
    await this.handleOwners(erc721Batch);
    await this.dalNFTTransferHistoryService.createCryptoPunksNFTTransferHistoryBatch(
      cryptopunkBatch,
    );
    await this.handleOwners(cryptopunkBatch);
    await this.dalNFTTransferHistoryService.createERC1155NFTTransferHistoryBatch(
      erc1155Batch,
    );
    const toBeInserted = R.uniqBy(R.prop('contractAddress'), allAddress);
    await this.nftCollectionService.insertIfNotThere(toBeInserted);
    await this.dalNFTTokensService.upsertTokens(tokens);
    const toBeInsertedTasks = R.uniqBy(
      R.props(['contractAddress', 'tokenId']),
      ownerTasks,
    );
    await this.nftTokenOwnersTaskService.upsertTasks(toBeInsertedTasks);
  }

  onError(error: Error, message: AWS.SQS.Message) {
    this.logger.log(`SQS error ${error.message}`);
    this.handleError(error, message, 'SQS');
  }

  onProcessingError(error: Error, message: AWS.SQS.Message) {
    this.logger.log(`Processing error ${error.message}`);
    this.handleError(error, message, 'Processing');
  }

  onTimeoutError(error: Error, message: AWS.SQS.Message) {
    this.logger.log(`Timeout error ${error.message}`);
    this.handleError(error, message, 'Timeout');
  }

  onMessageProcessed(message: AWS.SQS.Message) {
    this.nftBlockMonitorTaskService.removeNFTBlockMonitorTask(
      message.MessageId,
    );
    this.logger.log(`Messages ${message?.MessageId} have been processed`);
  }

  private handleError(error: Error, message: AWS.SQS.Message, type: string) {
    const receivedMessage = JSON.parse(message.Body) as ReceivedMessage;

    const nftBlockTask = {
      messageId: message.MessageId,
      blockNum: receivedMessage.blockNum,
    };

    this.nftBlockMonitorTaskService.updateNFTBlockMonitorTask({
      ...nftBlockTask,
      status: MessageStatus.error,
      errorMessage:
        `Error type: [${type}] - ${error.stack || error.message}` ||
        `Error type: [${type}] - ${JSON.stringify(error)}`,
    });
    this.deleteMessage(message);
  }

  private async deleteMessage(message: AWS.SQS.Message) {
    const deleteParams = {
      QueueUrl: this.configService.get('aws.queueUrl'),
      ReceiptHandle: message.ReceiptHandle,
    };

    try {
      await this.queue.deleteMessage(deleteParams).promise();
    } catch (err) {
      this.logger.log(`Deleting Message(${message?.MessageId}) ERROR`);
    }
  }
}
