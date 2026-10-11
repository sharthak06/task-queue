import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import type { Redis } from "ioredis";
import { createTestRedisClient, flushTestDb } from "./helpers/testRedis.js";
import { CLAIM_SCRIPT, FAIL_SCRIPT } from "../src/worker/scripts.js";

let client: Redis;

before(() => {
  client = createTestRedisClient(15);
});

beforeEach(async () => {
  await flushTestDb(client);
});

after(async () => {
  await client.quit();
});

test("CLAIM_SCRIPT does not claim a task scored in the future", async () => {
  const future = Date.now() + 60_000;
  await client.zadd("pending", future, JSON.stringify({ id: "a" }));

  const now = Date.now();
  const result = await client.eval(CLAIM_SCRIPT, 2, "pending", "claimed", String(now + 30_000), String(now));

  assert.equal(result, null);
});

test("CLAIM_SCRIPT moves a ready task from pending to claimed atomically", async () => {
  const taskJson = JSON.stringify({ id: "b" });
  await client.zadd("pending", Date.now(), taskJson);

  const now = Date.now();
  const result = await client.eval(CLAIM_SCRIPT, 2, "pending", "claimed", String(now + 30_000), String(now));

  assert.equal(result, taskJson);
  assert.equal(await client.zscore("pending", taskJson), null);
  assert.ok(await client.zscore("claimed", taskJson));
});

test("CLAIM_SCRIPT never lets two concurrent callers claim the same task", async () => {
  const taskCount = 50;
  const tasks = Array.from({ length: taskCount }, (_, i) => JSON.stringify({ id: `task-${i}` }));

  const pipeline = client.pipeline();
  for (const t of tasks) pipeline.zadd("pending", Date.now(), t);
  await pipeline.exec();

  const now = Date.now();
  const claims = await Promise.all(
    Array.from({ length: taskCount }, () =>
      client.eval(CLAIM_SCRIPT, 2, "pending", "claimed", String(now + 30_000), String(now)),
    ),
  );

  const claimed = claims.filter((c): c is string => c !== null);
  assert.equal(claimed.length, taskCount, "every task should be claimed exactly once");
  assert.equal(new Set(claimed).size, taskCount, "no task should be claimed twice");
});

test("FAIL_SCRIPT requeues with backoff when attempts remain", async () => {
  const task = { id: "c", type: "log", payload: {}, attempts: 0, maxAttempts: 3 };
  const taskJson = JSON.stringify(task);
  await client.zadd("claimed", Date.now(), taskJson);

  const now = Date.now();
  const outcome = await client.eval(FAIL_SCRIPT, 3, "claimed", "pending", "dlq", taskJson, String(now), "1000", "boom");

  assert.equal(outcome, 1);
  assert.equal(await client.zscore("claimed", taskJson), null);

  const pendingMembers = await client.zrange("pending", 0, -1);
  assert.equal(pendingMembers.length, 1);
  const requeued = JSON.parse(pendingMembers[0] as string) as typeof task;
  assert.equal(requeued.attempts, 1);

  const score = await client.zscore("pending", pendingMembers[0] as string);
  assert.ok(Number(score) >= now + 2000, "backoff score should be at least baseDelay * 2^1 in the future");
});

test("FAIL_SCRIPT sends to DLQ once maxAttempts is reached", async () => {
  const task = { id: "d", type: "log", payload: {}, attempts: 2, maxAttempts: 3 };
  const taskJson = JSON.stringify(task);
  await client.zadd("claimed", Date.now(), taskJson);

  const outcome = await client.eval(
    FAIL_SCRIPT,
    3,
    "claimed",
    "pending",
    "dlq",
    taskJson,
    String(Date.now()),
    "1000",
    "boom",
  );

  assert.equal(outcome, 2);
  assert.equal(await client.zcard("pending"), 0);
  const dlqEntries = await client.xrange("dlq", "-", "+");
  assert.equal(dlqEntries.length, 1);
});

test("FAIL_SCRIPT is a no-op if the task was already handled elsewhere (worker/reaper race)", async () => {
  const task = { id: "e", type: "log", payload: {}, attempts: 0, maxAttempts: 3 };
  const taskJson = JSON.stringify(task);
  await client.zadd("claimed", Date.now(), taskJson);

  const now = Date.now();
  const args = [FAIL_SCRIPT, 3, "claimed", "pending", "dlq", taskJson, String(now), "1000", "boom"] as const;

  const [first, second] = await Promise.all([client.eval(...args), client.eval(...args)]);

  assert.deepEqual([first, second].sort(), [0, 1], "exactly one call should win, the other must be a no-op");
});
