const path = require('node:path');

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const TIME_ZONE = process.env.APP_TIMEZONE || 'Asia/Shanghai';
const RETENTION_DAYS = Number(process.env.RETENTION_DAYS || 30);
const DATA_DIR = process.env.DATA_DIR
  ? path.resolve(process.env.DATA_DIR)
  : path.join(process.cwd(), 'data');

module.exports = {
  PORT,
  HOST,
  TIME_ZONE,
  RETENTION_DAYS,
  DATA_DIR
};
