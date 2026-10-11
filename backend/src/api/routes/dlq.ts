import type { Redis } from "ioredis";
import type { RequestHandler } from "express";
import { log } from "../../logger.js";

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 200;

export function createDlqHandler(client: Redis): RequestHandler {
  return async (req, res) => {
    try {
      const rawLimit = req.query["limit"];
      let limit = DEFAULT_LIMIT;
      if (typeof rawLimit === "string") {
        const parsed = Number(rawLimit);
        if (Number.isFinite(parsed) && parsed > 0) {
          limit = Math.min(Math.floor(parsed), MAX_LIMIT);
        }
      }

      const rawBefore = req.query["before"];
      const start = typeof rawBefore === "string" && rawBefore.trim() !== "" ? `(${rawBefore}` : "+";

      const entries = await client.xrevrange("dlq", start, "-", "COUNT", limit);
      const result = entries.map(([id, fields]) => {
        const obj: Record<string, string> = {};
        for (let i = 0; i < fields.length; i += 2) {
          const key = fields[i];
          const value = fields[i + 1];
          if (key !== undefined && value !== undefined) {
            obj[key] = value;
          }
        }
        return {
          id,
          taskId: obj["taskId"] ?? "",
          type: obj["type"] ?? "",
          error: obj["error"] ?? "",
          attempts: obj["attempts"] ?? "",
        };
      });

      const lastEntry = result[result.length - 1];
      const nextCursor = result.length === limit && lastEntry ? lastEntry.id : null;

      res.status(200).json({ entries: result, nextCursor });
    } catch (err) {
      log(`ERROR: /dlq: ${String(err)}`);
      res.status(500).json({ error: "internal error" });
    }
  };
}
