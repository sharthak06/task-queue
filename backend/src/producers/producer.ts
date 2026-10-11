import type { Redis } from "ioredis";
import { randomUUID } from "node:crypto";

import type { Task } from "../types/task.js";
import { MAX_ATTEMPTS } from "../config.js";
import { log } from "../logger.js";
import { setTaskStatus } from "../api/taskStatus.js";
import { createRedisClient } from "../redisClient.js";
import { isMainModule } from "../entrypoint.js";

export async function pushTask(
  client: Redis,
  type: string,
  payload: Record<string, unknown>,
  runAt: number = Date.now(),
): Promise<Task> {
  const task: Task = { id: randomUUID(), type, payload, attempts: 0, maxAttempts: MAX_ATTEMPTS };
  await client.zadd("pending", runAt, JSON.stringify(task));
  try {
    await setTaskStatus(client, task.id, { status: "pending", type: task.type, attempts: task.attempts });
  } catch (err) {
    log(`WARNING: failed to set task status for ${task.id}: ${String(err)}`);
  }
  log(`ENQUEUED: Task type: ${task.type} Task payload: ${JSON.stringify(task.payload)} Task id: ${task.id} Run at: ${new Date(runAt).toISOString()}`);
  return task;
}
async function main(): Promise<void> {
  const client = createRedisClient();
  await pushTask(client, "log", { message: "hello from producer" });
  await client.quit();
}

if (isMainModule(import.meta.url)) {
  main().catch(console.error);
}

