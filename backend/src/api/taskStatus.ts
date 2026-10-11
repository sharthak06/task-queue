import type { Redis } from "ioredis";
import { TASK_STATUS_TTL_SECONDS } from "../config.js";

export type TaskStatusValue = "pending" | "claimed" | "completed" | "failed_retrying" | "dlq";

export interface TaskStatusFields {
  status: TaskStatusValue;
  type: string;
  attempts: number;
  error?: string;
}

export async function setTaskStatus(client: Redis, id: string, fields: TaskStatusFields): Promise<void> {
  const updatedAt = new Date().toISOString();
  await client
    .multi()
    .hset(`task:${id}`, { ...fields, updatedAt })
    .expire(`task:${id}`, TASK_STATUS_TTL_SECONDS)
    .exec();
}
