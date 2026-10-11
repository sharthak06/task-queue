import { Redis } from "ioredis";

const TEST_REDIS_HOST = process.env["REDIS_TEST_HOST"] ?? "redis://127.0.0.1:6379";

// Node's test runner runs test files concurrently by default. Each test file
// gets its own dedicated DB index so a flushdb() in one file's beforeEach
// can never wipe data another file's test just seeded.
export function getTestRedisUrl(dbIndex: number): string {
  return `${TEST_REDIS_HOST}/${dbIndex}`;
}

export function createTestRedisClient(dbIndex: number): Redis {
  return new Redis(getTestRedisUrl(dbIndex));
}

export async function flushTestDb(client: Redis): Promise<void> {
  await client.flushdb();
}
