import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import type { Redis } from "ioredis";
import { createTestRedisClient, flushTestDb } from "./helpers/testRedis.js";
import { startReaper } from "../src/reaper/reaper.js";
import { readMetricCounters } from "../src/api/metrics.js";

let dbClient: Redis;

before(() => {
  dbClient = createTestRedisClient(12);
});

beforeEach(async () => {
  await flushTestDb(dbClient);
});

after(async () => {
  await dbClient.quit();
});

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 3000, intervalMs = 20): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error("waitFor: condition not met within timeout");
}

test("reclaims a stuck claimed task back to pending with backoff", async () => {
  const reaperClient = createTestRedisClient(12);
  const assertClient = createTestRedisClient(12);
  const controller = new AbortController();

  try {
    const task = { id: "r1", type: "log", payload: {}, attempts: 0, maxAttempts: 3 };
    // scored in the past, as if a worker claimed it and then died before finishing
    await assertClient.zadd("claimed", Date.now() - 1000, JSON.stringify(task));

    const reaperDone = startReaper(reaperClient, {
      signal: controller.signal,
      reaperIntervalMs: 30,
      baseDelayMs: 50,
    });

    await waitFor(async () => (await assertClient.zcard("claimed")) === 0);

    const pendingMembers = await assertClient.zrange("pending", 0, -1);
    assert.equal(pendingMembers.length, 1);
    const requeued = JSON.parse(pendingMembers[0] as string) as typeof task;
    assert.equal(requeued.attempts, 1);

    const counters = await readMetricCounters(assertClient);
    assert.equal(counters.requeued, 1);

    controller.abort();
    await reaperDone;
  } finally {
    controller.abort();
    await assertClient.quit();
  }
});

test("reclaims a stuck task straight to the DLQ once maxAttempts is reached", async () => {
  const reaperClient = createTestRedisClient(12);
  const assertClient = createTestRedisClient(12);
  const controller = new AbortController();

  try {
    const task = { id: "r2", type: "log", payload: {}, attempts: 2, maxAttempts: 3 };
    await assertClient.zadd("claimed", Date.now() - 1000, JSON.stringify(task));

    const reaperDone = startReaper(reaperClient, {
      signal: controller.signal,
      reaperIntervalMs: 30,
      baseDelayMs: 50,
    });

    await waitFor(async () => (await assertClient.xrange("dlq", "-", "+")).length > 0);

    assert.equal(await assertClient.zcard("claimed"), 0);
    assert.equal(await assertClient.zcard("pending"), 0);

    controller.abort();
    await reaperDone;
  } finally {
    controller.abort();
    await assertClient.quit();
  }
});
