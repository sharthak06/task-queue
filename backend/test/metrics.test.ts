import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import type { Redis } from "ioredis";
import { createTestRedisClient, flushTestDb } from "./helpers/testRedis.js";
import { incrementMetric, readMetricCounters } from "../src/api/metrics.js";
import { createApp } from "../src/api/app.js";

const DB_INDEX = 9;

let client: Redis;

before(() => {
  client = createTestRedisClient(DB_INDEX);
});

beforeEach(async () => {
  await flushTestDb(client);
});

after(async () => {
  await client.quit();
});

test("incrementMetric and readMetricCounters round-trip correctly", async () => {
  await incrementMetric(client, "completed");
  await incrementMetric(client, "completed");
  await incrementMetric(client, "requeued");
  await incrementMetric(client, "dlq");

  const counters = await readMetricCounters(client);
  assert.deepEqual(counters, { completed: 2, requeued: 1, dlq: 1 });
});

test("readMetricCounters defaults to zero when nothing has been recorded", async () => {
  const counters = await readMetricCounters(client);
  assert.deepEqual(counters, { completed: 0, requeued: 0, dlq: 0 });
});

test("GET /metrics reflects queue depth, DLQ size, and counters", async () => {
  await client.zadd("pending", Date.now(), JSON.stringify({ id: "p1" }));
  await client.zadd("pending", Date.now(), JSON.stringify({ id: "p2" }));
  await client.zadd("claimed", Date.now(), JSON.stringify({ id: "c1" }));
  await client.xadd("dlq", "*", "taskId", "d1", "type", "log", "error", "boom", "attempts", "5");

  await incrementMetric(client, "completed");
  await incrementMetric(client, "completed");
  await incrementMetric(client, "completed");
  await incrementMetric(client, "requeued");
  await incrementMetric(client, "dlq");

  const app = createApp(client);
  const res = await request(app).get("/metrics");

  assert.equal(res.status, 200);
  assert.equal(res.body.pending, 2);
  assert.equal(res.body.inFlight, 1);
  assert.equal(res.body.dlqSize, 1);
  assert.equal(res.body.succeeded, 3);
  assert.equal(res.body.requeued, 1);
  assert.equal(res.body.failed, 1);
  assert.equal(res.body.successRate, 3 / 4);
  assert.equal(res.body.failureRate, 1 / 4);
});

test("GET /metrics returns null rates when no tasks have finished yet", async () => {
  const app = createApp(client);
  const res = await request(app).get("/metrics");

  assert.equal(res.status, 200);
  assert.equal(res.body.succeeded, 0);
  assert.equal(res.body.failed, 0);
  assert.equal(res.body.successRate, null);
  assert.equal(res.body.failureRate, null);
});
