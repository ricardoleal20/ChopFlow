// Shared ioredis connection factory for the BullMQ driver + worker.
import IORedis from 'ioredis';

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379/0';

export function createConnection() {
  // BullMQ requires maxRetriesPerRequest: null on its own connections.
  return new IORedis(REDIS_URL, { maxRetriesPerRequest: null });
}

export { REDIS_URL };
