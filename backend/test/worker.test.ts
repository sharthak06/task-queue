import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { Server } from "node:http";
import type { AddressInfo } from "node:net";
import type { Redis } from "ioredis";
import { createTestRedisClient, flushTestDb } from "./helpers/testRedis.js";
import { startWorker, webhookHandler } from "../src/worker/worker.js";
import type { TaskHandler } from "../src/worker/worker.js";
import type { Task } from "../src/types/task.js";
import { readMetricCounters } from "../src/api/metrics.js";

let dbClient: Redis;

before(() => {
  dbClient = createTestRedisClient(13);
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

test("a registered handler success moves the task to completed", async () => {
  const workerClient = createTestRedisClient(13);
  const assertClient = createTestRedisClient(13);
  const controller = new AbortController();

  try {
    const handlers = new Map<string, TaskHandler>([["log", async () => {}]]);
    const workerDone = startWorker(workerClient, handlers, {
      signal: controller.signal,
      pollIntervalMs: 20,
      visibilityTimeoutMs: 5000,
      baseDelayMs: 50,
    });

    const task = { id: "w1", type: "log", payload: {}, attempts: 0, maxAttempts: 3 };
    await assertClient.zadd("pending", Date.now(), JSON.stringify(task));

    await waitFor(async () => (await assertClient.hgetall("task:w1"))["status"] === "completed");

    const counters = await readMetricCounters(assertClient);
    assert.equal(counters.completed, 1);

    controller.abort();
    await workerDone;
  } finally {
    controller.abort();
    await assertClient.quit();
  }
});

test("an unregistered task type is routed to the DLQ, not silently dropped", async () => {
  const workerClient = createTestRedisClient(13);
  const assertClient = createTestRedisClient(13);
  const controller = new AbortController();

  try {
    const handlers = new Map<string, TaskHandler>();
    const workerDone = startWorker(workerClient, handlers, {
      signal: controller.signal,
      pollIntervalMs: 20,
      visibilityTimeoutMs: 5000,
      baseDelayMs: 50,
    });

    const task = { id: "w2", type: "mystery", payload: {}, attempts: 0, maxAttempts: 1 };
    await assertClient.zadd("pending", Date.now(), JSON.stringify(task));

    await waitFor(async () => (await assertClient.xrange("dlq", "-", "+")).length > 0);

    assert.equal(await assertClient.zcard("pending"), 0);
    assert.equal(await assertClient.zcard("claimed"), 0);

    const entries = await assertClient.xrange("dlq", "-", "+");
    assert.equal(entries.length, 1);
    const fields = entries[0]?.[1] ?? [];
    const taskIdIndex = fields.indexOf("taskId");
    assert.equal(fields[taskIdIndex + 1], "w2");

    controller.abort();
    await workerDone;
  } finally {
    controller.abort();
    await assertClient.quit();
  }
});

test("a malformed pending entry doesn't stop the worker from processing later tasks", async () => {
  const workerClient = createTestRedisClient(13);
  const assertClient = createTestRedisClient(13);
  const controller = new AbortController();

  try {
    const handlers = new Map<string, TaskHandler>([["log", async () => {}]]);
    const workerDone = startWorker(workerClient, handlers, {
      signal: controller.signal,
      pollIntervalMs: 20,
      visibilityTimeoutMs: 5000,
      baseDelayMs: 50,
    });

    await assertClient.zadd("pending", Date.now(), "{not valid json");
    // give the worker a moment to hit and recover from the bad entry
    await new Promise((resolve) => setTimeout(resolve, 100));

    const task = { id: "w3", type: "log", payload: {}, attempts: 0, maxAttempts: 3 };
    await assertClient.zadd("pending", Date.now(), JSON.stringify(task));

    await waitFor(async () => (await assertClient.hgetall("task:w3"))["status"] === "completed");

    controller.abort();
    await workerDone;
  } finally {
    controller.abort();
    await assertClient.quit();
  }
});

test("a handler that always throws exhausts retries and lands in the DLQ with the right attempt count", async () => {
  const workerClient = createTestRedisClient(13);
  const assertClient = createTestRedisClient(13);
  const controller = new AbortController();

  try {
    const handlers = new Map<string, TaskHandler>([
      [
        "boom",
        async () => {
          throw new Error("handler failure");
        },
      ],
    ]);
    const workerDone = startWorker(workerClient, handlers, {
      signal: controller.signal,
      pollIntervalMs: 20,
      visibilityTimeoutMs: 5000,
      baseDelayMs: 50,
    });

    const task = { id: "w4", type: "boom", payload: {}, attempts: 0, maxAttempts: 2 };
    await assertClient.zadd("pending", Date.now(), JSON.stringify(task));

    await waitFor(async () => (await assertClient.xrange("dlq", "-", "+")).length > 0, 5000);

    const entries = await assertClient.xrange("dlq", "-", "+");
    assert.equal(entries.length, 1);
    const fields = entries[0]?.[1] ?? [];
    const attemptsIndex = fields.indexOf("attempts");
    assert.equal(fields[attemptsIndex + 1], "2");

    controller.abort();
    await workerDone;
  } finally {
    controller.abort();
    await assertClient.quit();
  }
});

function makeTask(overrides: Partial<Task> = {}): Task {
  return { id: "t1", type: "webhook", payload: {}, attempts: 0, maxAttempts: 3, ...overrides };
}

async function withTestServer(
  handleRequest: (body: unknown) => { status: number },
  run: (url: string) => Promise<void>,
): Promise<void> {
  const server: Server = createServer((req, res) => {
    let raw = "";
    req.on("data", (chunk: Buffer) => (raw += chunk));
    req.on("end", () => {
      const { status } = handleRequest(raw.length > 0 ? JSON.parse(raw) : {});
      res.writeHead(status).end();
    });
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const { port } = server.address() as AddressInfo;
    await run(`http://127.0.0.1:${port}/hook`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

test("webhookHandler POSTs the payload body and succeeds on a 2xx response", async () => {
  let received: unknown;
  await withTestServer(
    (body) => {
      received = body;
      return { status: 204 };
    },
    async (url) => {
      await webhookHandler(makeTask({ payload: { url, body: { event: "signup" } } }));
    },
  );
  assert.deepEqual(received, { event: "signup" });
});

test("webhookHandler throws on a non-2xx response, so the caller's retry path can handle it", async () => {
  await withTestServer(
    () => ({ status: 500 }),
    async (url) => {
      await assert.rejects(
        webhookHandler(makeTask({ payload: { url } })),
        /webhook POST to .* failed: 500/,
      );
    },
  );
});

test("webhookHandler rejects a payload missing a url without making a request", async () => {
  await assert.rejects(webhookHandler(makeTask({ payload: {} })), /requires a "url" string/);
});

test("a failing webhook task retries then lands in the DLQ, same as any other handler failure", async () => {
  const workerClient = createTestRedisClient(13);
  const assertClient = createTestRedisClient(13);
  const controller = new AbortController();

  await withTestServer(
    () => ({ status: 503 }),
    async (url) => {
      try {
        const handlers = new Map<string, TaskHandler>([["webhook", webhookHandler]]);
        const workerDone = startWorker(workerClient, handlers, {
          signal: controller.signal,
          pollIntervalMs: 20,
          visibilityTimeoutMs: 5000,
          baseDelayMs: 50,
        });

        const task = makeTask({ id: "w5", payload: { url }, maxAttempts: 2 });
        await assertClient.zadd("pending", Date.now(), JSON.stringify(task));

        await waitFor(async () => (await assertClient.xrange("dlq", "-", "+")).length > 0, 5000);

        const entries = await assertClient.xrange("dlq", "-", "+");
        assert.equal(entries.length, 1);
        const fields = entries[0]?.[1] ?? [];
        const taskIdIndex = fields.indexOf("taskId");
        assert.equal(fields[taskIdIndex + 1], "w5");

        controller.abort();
        await workerDone;
      } finally {
        controller.abort();
        await assertClient.quit();
      }
    },
  );
});
