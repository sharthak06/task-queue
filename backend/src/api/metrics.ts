import type { Redis } from "ioredis";

const METRICS_KEY = "metrics";

export type MetricName = "completed" | "requeued" | "dlq";

export async function incrementMetric(client: Redis, metric: MetricName): Promise<void> {
  await client.hincrby(METRICS_KEY, metric, 1);
}

export interface MetricCounters {
  completed: number;
  requeued: number;
  dlq: number;
}

export async function readMetricCounters(client: Redis): Promise<MetricCounters> {
  const raw = await client.hgetall(METRICS_KEY);
  return {
    completed: Number(raw["completed"] ?? 0),
    requeued: Number(raw["requeued"] ?? 0),
    dlq: Number(raw["dlq"] ?? 0),
  };
}
