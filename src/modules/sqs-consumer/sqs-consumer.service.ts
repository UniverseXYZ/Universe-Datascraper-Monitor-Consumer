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
  EmptyLogError,
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
import { BulkWriteError, TransferHistory } from '../ethereum/ethereum.types';
import { getLatestHistory, calculateOwners } from './owners.operators';

const abiCoder = new ethers.utils.AbiCoder();
const decodeAddress = (data: string) => {
  return abiCoder.decode(['address'], data)[0];
};

@Injectable()
export class SqsConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SqsConsumerService.name);
  public sqsConsumer: Consumer;
  public queue: AWS.SQS;
  private processingBlockNum: number;
  private blacklist: string[];
  private source: string;

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
    const source = this.configService.get('source');

    if (!region || !accessKeyId || !secretAccessKey || !source) {
      throw new Error(
        'Initialize AWS queue failed, please check required variables',
      );
    }

    if (source !== 'ARCHIVE' && source !== 'MONITOR') {
      throw new Error(`SOURCE has invalid value(${source})`);
    }

    this.source = source;

    AWS.config.update({
      region,
      accessKeyId,
      secretAccessKey,
    });
  }

  public onModuleInit() {
    this.logger.log('onModuleInit');
    //this.blacklist = this.configService.get('blacklist').split(',')

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
    const batchSize = Number(this.configService.get('mongodb.batchSize'));

    if (transferHistories.length === 0) return;

    this.logger.log(`[${this.processingBlockNum}] getting latest history`);
    const latestHistory = getLatestHistory(transferHistories);

    this.logger.log(`[${this.processingBlockNum}] getting ERC721 token owners`);
    const owners = await this.nftTokenOwnerService.getERC721NFTTokenOwners(
      latestHistory.map((x) => ({
        contractAddress: x.contractAddress,
        tokenId: x.tokenId,
      })),
    );

    const { toBeInsertedOwners, toBeUpdatedOwners } = calculateOwners(
      latestHistory,
      owners,
    );

    this.logger.log(
      `[${this.processingBlockNum}] updating token owners, ${toBeInsertedOwners.length} owners | Batch size: ${batchSize}`,
    );
    await this.nftTokenOwnerService.upsertERC721NFTTokenOwners(
      toBeInsertedOwners,
      batchSize,
    );

    this.logger.log(
      `[${this.processingBlockNum}] upserting token owners, ${toBeUpdatedOwners.length} owners | Batch size: ${batchSize}`,
    );
    await this.nftTokenOwnerService.updateERC721NFTTokenOwners(
      toBeUpdatedOwners,
      batchSize,
    );
  }

  async handleMessage(message: AWS.SQS.Message) {
    this.logger.log(`Consumer handle message id:(${message.MessageId})`);
    const currentTimestamp = new Date().getTime() / 1000;
    const receivedMessage = JSON.parse(message.Body) as ReceivedMessage;
    const batchSize = Number(this.configService.get('mongodb.batchSize'));

    const nftBlockTask = {
      messageId: message.MessageId,
      blockNum: receivedMessage.blockNum,
    };

    this.processingBlockNum = receivedMessage.blockNum;

    this.logger.log(
      `[${this.processingBlockNum}] setting message id:(${message.MessageId}) as processing`,
    );
    await this.nftBlockMonitorTaskService.updateNFTBlockMonitorTask({
      ...nftBlockTask,
      status: MessageStatus.processing,
    });
    const ERC721Transfer =
      '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
    const CryptoPunksPunkAssign =
      '0x8a0e37b73a0d9c82e205d4d1a3ff3d0b57ce5f4d7bccf6bac03336dc101cb7ba';
    const CryptoPunksPunkTransfer =
      '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8';
    const CryptoPunksPunkBought =
      '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3';
    const ERC1155TransferSingle =
      '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62';
    const ERC1155TransferBatch =
      '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb';

    this.logger.log(`[${this.processingBlockNum}] getting logs in block`);
    const logInBlock = await this.etherService.getLogsInBlock(
      nftBlockTask.blockNum,
    );
    this.logger.log(
      `[${this.processingBlockNum}] found ${logInBlock.length} logs in this block may contain NFT transfers`,
    );

    if (logInBlock.length === 0) {
      this.logger.log(
        `[${this.processingBlockNum}] no logs found in this block`,
      );
      throw new EmptyLogError('No logs found in this block');
    }

    const addresses = logInBlock.map((x) => x.address);
    this.logger.log(
      `[${this.processingBlockNum}] retrieving contracts from block`,
    );
    const allAddress = await this.etherService.getContractsInBlock(
      addresses,
      receivedMessage.blockNum,
    );
    const cryptopunkBatch = [];
    const erc721Batch = [];
    const erc1155Batch = [];
    const tokens = [];
    const ownerTasks = [];

    for (const x of logInBlock) {
      const { address, data, topics, blockNumber, transactionHash, logIndex } =
        x;
      //if(this.blacklist.includes(address)) continue;
      switch (topics[0]) {
        case CryptoPunksPunkAssign:
          if (R.prop(address)(allAddress) != 'CryptoPunks') {
            break;
          }
          const punkIndexAssign = ethers.BigNumber.from(data).toString();
          cryptopunkBatch.push({
            contractAddress: address,
            blockNum: blockNumber,
            hash: transactionHash,
            logIndex: logIndex,
            from: ethers.constants.AddressZero,
            to: decodeAddress(topics[1]),
            tokenId: punkIndexAssign,
            value: 1,
            cryptopunks: {
              punkIndex: punkIndexAssign,
            },
            category: 'CryptoPunks',
          });
          tokens.push({
            contractAddress: address,
            tokenType: 'CryptoPunks',
            tokenId: punkIndexAssign,
            source: this.source,
          });
          break;
        case CryptoPunksPunkTransfer:
          if (R.prop(address)(allAddress) != 'CryptoPunks') {
            break;
          }
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
          tokens.push({
            contractAddress: address,
            tokenType: 'CryptoPunks',
            tokenId: punkIndex,
            source: this.source,
          });
          break;
        case CryptoPunksPunkBought:
          if (R.prop(address)(allAddress) != 'CryptoPunks') {
            break;
          }
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
          tokens.push({
            contractAddress: address,
            tokenType: 'CryptoPunks',
            tokenId: _punkIndex,
            source: this.source,
          });
          break;
        case ERC721Transfer:
          // erc20 and erc721 topic is the same, thus we can differentiate them by checking the length of topics
          if (topics.length < 4) {
            break;
          }
          if (R.prop(address)(allAddress) != 'ERC721') {
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
          tokens.push({
            contractAddress: address,
            tokenType: 'ERC721',
            tokenId: erc721TokenId,
            source: this.source,
          });
          break;
        case ERC1155TransferSingle:
          if (R.prop(address)(allAddress) != 'ERC1155') {
            break;
          }
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
          tokens.push({
            contractAddress: address,
            tokenType: 'ERC1155',
            tokenId: _id.toString(),
            value: ethers.BigNumber.from(_value).toString(),
            transactionHash: transactionHash,
            source: this.source,
          });
          ownerTasks.push({
            contractAddress: address,
            tokenType: 'ERC1155',
            tokenId: _id.toString(),
            taskId: uuidv4(),
          });
          break;
        case ERC1155TransferBatch:
          if (R.prop(address)(allAddress) != 'ERC1155') {
            break;
          }
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
              source: this.source,
            });
            ownerTasks.push({
              contractAddress: address,
              tokenType: 'ERC1155',
              tokenId: tokenId.toString(),
              taskId: uuidv4(),
            });
          });
          break;
        default:
          break;
      }
    }

    try {
      // ERC721
      this.logger.log(
        `[${this.processingBlockNum}] Bulk Writting ERC721 transfers: ${erc721Batch.length} histories | Batch size: ${batchSize}`,
      );
      await this.dalNFTTransferHistoryService.createERC721NFTTransferHistoryBatch(
        erc721Batch,
        batchSize,
      );
      await this.handleOwners(erc721Batch);

      // CryptoPunks
      this.logger.log(
        `[${this.processingBlockNum}] Bulk Writting CryptoPunks transfers: ${cryptopunkBatch.length} histories | Batch size: ${batchSize}`,
      );
      await this.dalNFTTransferHistoryService.createCryptoPunksNFTTransferHistoryBatch(
        cryptopunkBatch,
        batchSize,
      );
      await this.handleOwners(cryptopunkBatch);

      // ERC1155
      this.logger.log(
        `[${this.processingBlockNum}] Bulk Writting ERC1155 transfers: ${erc1155Batch.length} histories | Batch size: ${batchSize}`,
      );
      await this.dalNFTTransferHistoryService.createERC1155NFTTransferHistoryBatch(
        erc1155Batch,
        batchSize,
      );

      const allAddressKeys = R.keys(allAddress);
      const allAddressTypes = R.values(allAddress);
      const toBeInserted = R.zipWith(
        (a, b) => {
          return { contractAddress: a, tokenType: b };
        },
        allAddressKeys,
        allAddressTypes,
      );

      // collections
      this.logger.log(
        `[${this.processingBlockNum}] Bulk Writting Collections: ${toBeInserted.length} Collections`,
      );
      await this.nftCollectionService.insertIfNotThere(
        toBeInserted,
        batchSize,
        this.source,
      );

      // tokens
      this.logger.log(
        `[${this.processingBlockNum}] Bulk Writting Tokens: ${tokens.length} Tokens | Batch size: ${batchSize}`,
      );
      await this.dalNFTTokensService.upsertTokens(tokens, batchSize);

      const seen = Object.create(null);

      const toBeInsertedTasks = ownerTasks.filter((o) => {
        var key = ['contractAddress', 'tokenId'].map((k) => o[k]).join('|');
        if (!seen[key]) {
          seen[key] = true;
          return true;
        }
      });

      // token owners task
      this.logger.log(
        `[${this.processingBlockNum}] Inserting TokenOwnerTask: ${toBeInsertedTasks.length} Tasks`,
      );
      await this.nftTokenOwnersTaskService.upsertTasks(
        toBeInsertedTasks,
        batchSize,
      );
    } catch (e) {
      this.handleDBError(e);
    } finally {
      const endTimestamp = new Date().getTime() / 1000;
      this.logger.log(
        `[${receivedMessage.blockNum}] total processing time spent: ${
          endTimestamp - currentTimestamp
        } seconds`,
      );
    }
  }

  handleDBError(error: Error) {
    if (error.stack && error.stack.includes('E11000 duplicate key error')) {
      throw new BulkWriteError(error.message, error.stack);
    } else {
      throw error;
    }
  }

  async onError(error: Error, message: AWS.SQS.Message) {
    this.logger.error(
      `[${this.processingBlockNum}] SQS error ${error.message}`,
    );
    await this.handleError(error, message, 'SQS');
  }

  async onProcessingError(error: Error, message: AWS.SQS.Message) {
    this.logger.error(
      `[${this.processingBlockNum}] Processing error ${error.message}`,
    );
    await this.handleError(error, message, 'Processing');
  }

  async onTimeoutError(error: Error, message: AWS.SQS.Message) {
    this.logger.error(
      `[${this.processingBlockNum}] Timeout error ${error.message}`,
    );
    await this.handleError(error, message, 'Timeout');
  }

  async onMessageProcessed(message: AWS.SQS.Message) {
    await this.nftBlockMonitorTaskService.removeNFTBlockMonitorTask(
      message.MessageId,
    );
    this.logger.log(
      `[${this.processingBlockNum}] Messages ${message?.MessageId} have been processed`,
    );
  }

  private async handleError(
    error: Error,
    message: AWS.SQS.Message,
    type: string,
  ) {
    const receivedMessage = JSON.parse(message.Body) as ReceivedMessage;

    const nftBlockTask = {
      messageId: message.MessageId,
      blockNum: receivedMessage.blockNum,
    };

    let status = MessageStatus.error;
    let errorMessage =
      `Error type: [${type}] - ${error.stack || error.message}` ||
      `Error type: [${type}] - ${JSON.stringify(error)}`;

    if (error instanceof BulkWriteError) {
      // retry status. In case of duplicate message, we need retry/resend message. Its better than leave this message back to queue.
      status = MessageStatus.retry;
      errorMessage = null; // no need of error message in this case
    } else if (error instanceof EmptyLogError) {
      // empty status. Its possible for the block don't have any tx yet and we need persist it in this case and do manual check later
      status = MessageStatus.empty;
      errorMessage = null; // no need of error message in this case
    }

    await this.setTaskStatus(nftBlockTask, status, errorMessage);
    this.deleteMessage(message);
  }

  private async setTaskStatus(
    nftBlockTask: any,
    status: string,
    errorMessage: string,
  ) {
    await this.nftBlockMonitorTaskService.updateNFTBlockMonitorTask({
      ...nftBlockTask,
      status,
      errorMessage,
    });
  }

  private async deleteMessage(message: AWS.SQS.Message) {
    const deleteParams = {
      QueueUrl: this.configService.get('aws.queueUrl'),
      ReceiptHandle: message.ReceiptHandle,
    };

    try {
      await this.queue.deleteMessage(deleteParams).promise();
    } catch (err) {
      this.logger.log(
        `[${this.processingBlockNum}] Deleting Message(${message?.MessageId}) ERROR`,
      );
    }
  }
}
