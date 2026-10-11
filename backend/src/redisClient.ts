import "dotenv/config";
import { Redis } from "ioredis";

export function createRedisClient(): Redis {
  const url = process.env["REDIS_URL"] ?? "redis://127.0.0.1:6379";
  return new Redis(url);
}
