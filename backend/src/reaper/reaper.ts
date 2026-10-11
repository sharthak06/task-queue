import type { Redis } from "ioredis";
import type { Task } from "../types/task.js";
import { createRedisClient } from "../redisClient.js";
import { FAIL_SCRIPT } from "../worker/scripts.js";
import { REAPER_INTERVAL_MS, BASE_DELAY_MS } from "../config.js";
import { log } from "../logger.js";
import { setTaskStatus } from "../api/taskStatus.js";
import { incrementMetric } from "../api/metrics.js";
import { createShutdownSignal, abortableSleep } from "../shutdown.js";
import { isMainModule } from "../entrypoint.js";

export interface ReaperOptions {
  signal?: AbortSignal;
  reaperIntervalMs?: number;
  baseDelayMs?: number;
}

async function runOnce(client: Redis, baseDelayMs: number): Promise<void> {
  const now = Date.now();
  const expired = await client.zrangebyscore("claimed", "-inf", now);

  for (const taskJson of expired) {
    const outcome = (await client.eval(
      FAIL_SCRIPT,
      3,
      "claimed",
      "pending",
      "dlq",
      taskJson,
      String(now),
      String(baseDelayMs),
      "visibility timeout exceeded",
    )) as number;

    if (outcome === 0) continue;

    try {
      const { id, type, attempts } = JSON.parse(taskJson) as Task;
      if (outcome === 1) {
        log(`REAPED: Task id: ${id} was stuck, requeued`);
        try {
          await setTaskStatus(client, id, { status: "failed_retrying", type, attempts: attempts + 1 });
        } catch (statusErr) {
          log(`WARNING: failed to set task status for ${id}: ${String(statusErr)}`);
        }
        try {
          await incrementMetric(client, "requeued");
        } catch (metricErr) {
          log(`WARNING: failed to increment metric for ${id}: ${String(metricErr)}`);
        }
      } else if (outcome === 2) {
        log(`DLQ: Task type: ${type} Task id: ${id} Error: visibility timeout exceeded`);
        try {
          await setTaskStatus(client, id, { status: "dlq", type, attempts: attempts + 1, error: "visibility timeout exceeded" });
        } catch (statusErr) {
          log(`WARNING: failed to set task status for ${id}: ${String(statusErr)}`);
        }
        try {
          await incrementMetric(client, "dlq");
        } catch (metricErr) {
          log(`WARNING: failed to increment metric for ${id}: ${String(metricErr)}`);
        }
      }
    } catch {
      log("[reaper] processed task (unparseable json)");
    }
  }
}

export async function startReaper(client: Redis, options: ReaperOptions = {}): Promise<void> {
  const {
    signal = createShutdownSignal(),
    reaperIntervalMs = REAPER_INTERVAL_MS,
    baseDelayMs = BASE_DELAY_MS,
  } = options;

  log(`Reaper started, checking every ${reaperIntervalMs / 1000} seconds...`);

  while (!signal.aborted) {
    await runOnce(client, baseDelayMs).catch((err) => log(`ERROR: reaper runOnce failed: ${String(err)}`));
    if (signal.aborted) break;
    await abortableSleep(reaperIntervalMs, signal);
  }

  log("Reaper shutting down...");
  await client.quit();
}

async function main(): Promise<void> {
  const client = createRedisClient();
  await startReaper(client);
}

if (isMainModule(import.meta.url)) {
  main().catch(console.error);
}
