export default () => ({
  mongodb: {
    uri: process.env.MONGODB_URI,
    batchSize: process.env.BATCH_SIZE
  },
  port: process.env.APP_PORT,
  app_env: process.env.APP_ENV,
  alchemy_token: process.env.ALCHEMY_TOKEN,
  chainstack_url: process.env.CHAINSTACK_URL,
  quicknode_url: process.env.QUICKNODE_URL,
  blacklist: process.env.BLACKLIST,
  ethereum_network: process.env.ETHEREUM_NETWORK,
  session_secret: process.env.SESSION_SECRET,
  infura: {
    project_id: process.env.INFURA_PROJECT_ID,
    project_secret: process.env.INFURA_PROJECT_SECRET,
  },
  aws: {
    region: process.env.AWS_REGION,
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    queueUrl: process.env.AWS_QUEUE_URL,
  },
  etherscan_api_key: process.env.ETHERSCAN_API_KEY,
  source: process.env.SOURCE
});
