import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { spawn } from "node:child_process";
import type { ChildProcessWithoutNullStreams } from "node:child_process";
import type { Redis } from "ioredis";
import { createTestRedisClient, flushTestDb, getTestRedisUrl } from "./helpers/testRedis.js";

const PROJECT_ROOT = path.resolve(fileURLToPath(import.meta.url), "..", "..");
const DB_INDEX = 10;
const TEST_REDIS_URL = getTestRedisUrl(DB_INDEX);

let dbClient: Redis;

before(() => {
  dbClient = createTestRedisClient(DB_INDEX);
});

beforeEach(async () => {
  await flushTestDb(dbClient);
});

after(async () => {
  await dbClient.quit();
});

function spawnScript(relativeScriptPath: string): ChildProcessWithoutNullStreams {
  // Spawn via `node --import tsx/esm`, the same mechanism the npm test script
  // uses, instead of node_modules/.bin/tsx directly: that shim has no
  // Windows-executable form (only .cmd/.ps1 siblings), so a plain spawn()
  // fails with ENOENT on Windows.
  return spawn(process.execPath, ["--import", "tsx/esm", relativeScriptPath], {
    cwd: PROJECT_ROOT,
    env: { ...process.env, REDIS_URL: TEST_REDIS_URL },
  });
}

function waitForLogLine(child: ChildProcessWithoutNullStreams, matcher: string, timeoutMs = 5000): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`timed out waiting for log line matching "${matcher}"`));
    }, timeoutMs);
    const onData = (chunk: Buffer): void => {
      if (chunk.toString().includes(matcher)) {
        clearTimeout(timer);
        child.stdout.off("data", onData);
        resolve();
      }
    };
    child.stdout.on("data", onData);
  });
}

function killAndWait(child: ChildProcessWithoutNullStreams, timeoutMs = 3000): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      resolve();
    }, timeoutMs);
    child.once("exit", () => {
      clearTimeout(timer);
      resolve();
    });
    child.kill("SIGTERM");
  });
}

test("starting the reaper alone does not enqueue a spurious task", async () => {
  const assertClient = createTestRedisClient(DB_INDEX);
  const child = spawnScript("src/reaper/reaper.ts");

  try {
    await waitForLogLine(child, "Reaper started", 15_000);
    // give any accidental import-time side effect a moment to happen
    await new Promise((resolve) => setTimeout(resolve, 300));

    assert.equal(await assertClient.zcard("pending"), 0);
  } finally {
    await killAndWait(child);
    await assertClient.quit();
  }
});

test("starting the API server alone does not enqueue a spurious task", async () => {
  const assertClient = createTestRedisClient(DB_INDEX);
  const child = spawnScript("src/api/server.ts");

  try {
    await waitForLogLine(child, "API server listening", 15_000);
    await new Promise((resolve) => setTimeout(resolve, 300));

    assert.equal(await assertClient.zcard("pending"), 0);
  } finally {
    await killAndWait(child);
    await assertClient.quit();
  }
});
