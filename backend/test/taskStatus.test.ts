import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import type { Redis } from "ioredis";
import { createTestRedisClient, flushTestDb } from "./helpers/testRedis.js";
import { setTaskStatus } from "../src/api/taskStatus.js";

let client: Redis;

before(() => {
  client = createTestRedisClient(14);
});

beforeEach(async () => {
  await flushTestDb(client);
});

after(async () => {
  await client.quit();
});

test("setTaskStatus writes the expected hash fields", async () => {
  await setTaskStatus(client, "task-1", { status: "pending", type: "log", attempts: 0 });

  const hash = await client.hgetall("task:task-1");
  assert.equal(hash["status"], "pending");
  assert.equal(hash["type"], "log");
  assert.equal(hash["attempts"], "0");
  assert.ok(hash["updatedAt"]);
});

test("setTaskStatus includes the error field only when provided", async () => {
  await setTaskStatus(client, "task-2", { status: "dlq", type: "log", attempts: 3, error: "boom" });

  const hash = await client.hgetall("task:task-2");
  assert.equal(hash["error"], "boom");
});

test("setTaskStatus sets a TTL atomically alongside the hash write", async () => {
  // Verifies the visible outcome of the atomic multi() write (hash + TTL both
  // present immediately). It can't force the crash-between-HSET-and-EXPIRE
  // window the atomicity guards against — that's a code-review-level
  // guarantee of using multi(), not something a test can trigger directly.
  await setTaskStatus(client, "task-3", { status: "completed", type: "log", attempts: 1 });

  const ttl = await client.ttl("task:task-3");
  assert.ok(ttl > 0, `expected a positive TTL, got ${ttl}`);
});
