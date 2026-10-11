import type { Redis } from "ioredis";
import type { RequestHandler } from "express";
import { log } from "../../logger.js";
import { readMetricCounters } from "../metrics.js";

export function createMetricsHandler(client: Redis): RequestHandler {
  return async (_req, res) => {
    try {
      const [pending, inFlight, dlqSize, counters] = await Promise.all([
        client.zcard("pending"),
        client.zcard("claimed"),
        client.xlen("dlq"),
        readMetricCounters(client),
      ]);

      const finished = counters.completed + counters.dlq;

      res.status(200).json({
        pending,
        inFlight,
        dlqSize,
        succeeded: counters.completed,
        requeued: counters.requeued,
        failed: counters.dlq,
        successRate: finished === 0 ? null : counters.completed / finished,
        failureRate: finished === 0 ? null : counters.dlq / finished,
      });
    } catch (err) {
      log(`ERROR: /metrics: ${String(err)}`);
      res.status(500).json({ error: "internal error" });
    }
  };
}
