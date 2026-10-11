import type { Redis } from "ioredis";
import type { Task } from "../types/task.js";
import { createRedisClient } from "../redisClient.js";
import { CLAIM_SCRIPT, FAIL_SCRIPT } from "./scripts.js";
import { VISIBILITY_TIMEOUT_MS, POLL_INTERVAL_MS, BASE_DELAY_MS } from "../config.js";
import { log } from "../logger.js";
import { setTaskStatus } from "../api/taskStatus.js";
import { incrementMetric } from "../api/metrics.js";
import { createShutdownSignal, abortableSleep } from "../shutdown.js";
import { isMainModule } from "../entrypoint.js";

export type TaskHandler = (task: Task) => Promise<void>;

export const webhookHandler: TaskHandler = async (task) => {
  const { url, body } = task.payload as { url?: unknown; body?: unknown };
  if (typeof url !== "string" || url.trim() === "") {
    throw new Error('webhook task payload requires a "url" string');
  }

  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body ?? {}),
  });

  if (!res.ok) {
    throw new Error(`webhook POST to ${url} failed: ${res.status} ${res.statusText}`);
  }
};

export interface WorkerOptions {
  signal?: AbortSignal;
  pollIntervalMs?: number;
  visibilityTimeoutMs?: number;
  baseDelayMs?: number;
}

async function handleFailure(
  client: Redis,
  workerId: string,
  taskJson: string,
  err: string,
  baseDelayMs: number,
): Promise<void> {
  const { id, type, attempts, maxAttempts } = JSON.parse(taskJson) as Task;
  const now = Date.now();

  const outcome = (await client.eval(
    FAIL_SCRIPT,
    3,
    "claimed",
    "pending",
    "dlq",
    taskJson,
    String(now),
    String(baseDelayMs),
    err,
  )) as number;

  if (outcome === 0) {
    log(`FAILED: Worker ${workerId} Task type: ${type} Task id: ${id} Error: ${err} (task already handled elsewhere, no action taken)`);
    return;
  }

  const newAttempts = attempts + 1;
  log(`FAILED: Worker ${workerId} Task type: ${type} Task id: ${id} Error: ${err} Retries left: ${maxAttempts - newAttempts}`);

  if (outcome === 1) {
    const backoffUntil = new Date(now + baseDelayMs * Math.pow(2, newAttempts)).toISOString();
    log(`REQUEUED: Task id: ${id} Backoff until: ${backoffUntil}`);
    try {
      await setTaskStatus(client, id, { status: "failed_retrying", type, attempts: newAttempts });
    } catch (statusErr) {
      log(`WARNING: failed to set task status for ${id}: ${String(statusErr)}`);
    }
    try {
      await incrementMetric(client, "requeued");
    } catch (metricErr) {
      log(`WARNING: failed to increment metric for ${id}: ${String(metricErr)}`);
    }
  } else if (outcome === 2) {
    log(`DLQ: Task type: ${type} Task id: ${id} Error: ${err}`);
    try {
      await setTaskStatus(client, id, { status: "dlq", type, attempts: newAttempts, error: err });
    } catch (statusErr) {
      log(`WARNING: failed to set task status for ${id}: ${String(statusErr)}`);
    }
    try {
      await incrementMetric(client, "dlq");
    } catch (metricErr) {
      log(`WARNING: failed to increment metric for ${id}: ${String(metricErr)}`);
    }
  }
}

export async function startWorker(
  client: Redis,
  handlers: Map<string, TaskHandler>,
  options: WorkerOptions = {},
): Promise<void> {
  const {
    signal = createShutdownSignal(),
    pollIntervalMs = POLL_INTERVAL_MS,
    visibilityTimeoutMs = VISIBILITY_TIMEOUT_MS,
    baseDelayMs = BASE_DELAY_MS,
  } = options;

  const workerId = Math.random().toString(36).slice(2, 8);
  log(`Worker ${workerId} started, waiting for tasks...`);

  while (!signal.aborted) {
    try {
      const now = Date.now();
      const deadline = now + visibilityTimeoutMs;
      const result = (await client.eval(
        CLAIM_SCRIPT,
        2,
        "pending",
        "claimed",
        String(deadline),
        String(now),
      )) as string | null;

      if (result === null) {
        await abortableSleep(pollIntervalMs, signal);
        continue;
      }

      const task = JSON.parse(result) as Task;
      log(`CLAIMED: Worker ${workerId} Task type: ${task.type} Task id: ${task.id}`);

      try {
        await setTaskStatus(client, task.id, { status: "claimed", type: task.type, attempts: task.attempts });
      } catch (statusErr) {
        log(`WARNING: failed to set task status for ${task.id}: ${String(statusErr)}`);
      }

      const handler = handlers.get(task.type);

      if (!handler) {
        await handleFailure(client, workerId, result, `no handler registered for task type "${task.type}"`, baseDelayMs);
        continue;
      }

      try {
        await handler(task);
        await client.zrem("claimed", result);
        log(`SUCCESS: Worker ${workerId} Task type: ${task.type} Task payload: ${JSON.stringify(task.payload)} Retries left: ${task.maxAttempts - task.attempts}`);
        try {
          await setTaskStatus(client, task.id, { status: "completed", type: task.type, attempts: task.attempts });
        } catch (statusErr) {
          log(`WARNING: failed to set task status for ${task.id}: ${String(statusErr)}`);
        }
        try {
          await incrementMetric(client, "completed");
        } catch (metricErr) {
          log(`WARNING: failed to increment metric for ${task.id}: ${String(metricErr)}`);
        }
      } catch (err) {
        await handleFailure(client, workerId, result, String(err), baseDelayMs);
      }
    } catch (err) {
      log(`ERROR: Worker ${workerId} loop iteration failed: ${String(err)}`);
      await abortableSleep(pollIntervalMs, signal);
    }
  }

  log(`Worker ${workerId} shutting down...`);
  await client.quit();
}

async function main(): Promise<void> {
  const client = createRedisClient();

  const handlers = new Map<string, TaskHandler>([
    [
      "log",
      async (task) => {
        log(`[log] task ${task.id}: ${JSON.stringify(task.payload)}`);
      },
    ],
    ["webhook", webhookHandler],
  ]);

  await startWorker(client, handlers);
}

if (isMainModule(import.meta.url)) {
  main().catch(console.error);
}
