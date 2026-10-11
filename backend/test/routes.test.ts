import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import type { Redis } from "ioredis";
import { createTestRedisClient, flushTestDb } from "./helpers/testRedis.js";
import { createApp } from "../src/api/app.js";

let client: Redis;

before(() => {
  client = createTestRedisClient(11);
});

beforeEach(async () => {
  await flushTestDb(client);
});

after(async () => {
  await client.quit();
});

test("POST /enqueue rejects a missing type", async () => {
  const app = createApp(client);
  const res = await request(app).post("/enqueue").send({ payload: {} });
  assert.equal(res.status, 400);
});

test("POST /enqueue rejects a non-object payload", async () => {
  const app = createApp(client);
  const res = await request(app).post("/enqueue").send({ type: "log", payload: "not-an-object" });
  assert.equal(res.status, 400);
});

test("POST /enqueue succeeds and returns a taskId", async () => {
  const app = createApp(client);
  const res = await request(app).post("/enqueue").send({ type: "log", payload: { msg: "hi" } });
  assert.equal(res.status, 202);
  assert.ok(res.body.taskId);
});

test("POST /enqueue rejects a malformed runAt", async () => {
  const app = createApp(client);
  const res = await request(app).post("/enqueue").send({ type: "log", payload: {}, runAt: "not-a-date" });
  assert.equal(res.status, 400);
});

test("POST /enqueue with a future runAt schedules the task instead of making it immediately claimable", async () => {
  const app = createApp(client);
  const runAt = new Date(Date.now() + 60_000);
  const res = await request(app).post("/enqueue").send({ type: "log", payload: {}, runAt: runAt.toISOString() });
  assert.equal(res.status, 202);

  const members = await client.zrange("pending", 0, -1);
  assert.equal(members.length, 1);
  const score = await client.zscore("pending", members[0] as string);
  assert.equal(Number(score), runAt.getTime());
});

test("GET /tasks/:id returns 404 for an unknown task", async () => {
  const app = createApp(client);
  const res = await request(app).get("/tasks/does-not-exist");
  assert.equal(res.status, 404);
});

test("GET /tasks/:id returns the task status once it exists", async () => {
  const app = createApp(client);
  const enqueueRes = await request(app).post("/enqueue").send({ type: "log", payload: {} });
  const taskId = enqueueRes.body.taskId as string;

  const res = await request(app).get(`/tasks/${taskId}`);
  assert.equal(res.status, 200);
  assert.equal(res.body.status, "pending");
});

test("GET /health returns ok", async () => {
  const app = createApp(client);
  const res = await request(app).get("/health");
  assert.equal(res.status, 200);
  assert.equal(res.body.status, "ok");
});

test("GET /dlq returns entries newest-first with cursor pagination", async () => {
  for (let i = 1; i <= 3; i++) {
    await client.xadd("dlq", "*", "taskId", `t${i}`, "type", "log", "error", `boom${i}`, "attempts", "5");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }

  const app = createApp(client);
  const firstPage = await request(app).get("/dlq?limit=2");
  assert.equal(firstPage.status, 200);
  assert.equal(firstPage.body.entries.length, 2);
  assert.equal(firstPage.body.entries[0].taskId, "t3");
  assert.equal(firstPage.body.entries[1].taskId, "t2");
  assert.ok(firstPage.body.nextCursor);

  const secondPage = await request(app).get(`/dlq?limit=2&before=${firstPage.body.nextCursor}`);
  assert.equal(secondPage.status, 200);
  assert.equal(secondPage.body.entries.length, 1);
  assert.equal(secondPage.body.entries[0].taskId, "t1");
  assert.equal(secondPage.body.nextCursor, null);
});
