import type { Redis } from "ioredis";
import type { RequestHandler } from "express";
import { log } from "../../logger.js";

export function createHealthHandler(client: Redis): RequestHandler {
  return async (_req, res) => {
    try {
      await client.ping();
      res.status(200).json({ status: "ok" });
    } catch (err) {
      log(`WARNING: /health: redis unreachable: ${String(err)}`);
      res.status(503).json({ status: "unavailable" });
    }
  };
}
