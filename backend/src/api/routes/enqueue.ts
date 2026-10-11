import type { Redis } from "ioredis";
import type { RequestHandler } from "express";
import { pushTask } from "../../producers/producer.js";
import { log } from "../../logger.js";

export function createEnqueueHandler(client: Redis): RequestHandler {
  return async (req, res) => {
    try {
      const body = req.body as unknown;
      if (typeof body !== "object" || body === null || Array.isArray(body)) {
        res.status(400).json({ error: "request body must be a JSON object" });
        return;
      }

      const { type, payload, runAt } = body as Record<string, unknown>;

      if (typeof type !== "string" || type.trim() === "") {
        res.status(400).json({ error: "type must be a non-empty string" });
        return;
      }

      if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
        res.status(400).json({ error: "payload must be an object" });
        return;
      }

      let runAtMs: number | undefined;
      if (runAt !== undefined) {
        if (typeof runAt !== "string" || Number.isNaN(Date.parse(runAt))) {
          res.status(400).json({ error: "runAt must be an ISO 8601 date string" });
          return;
        }
        runAtMs = Date.parse(runAt);
      }

      const task = await pushTask(client, type, payload as Record<string, unknown>, runAtMs);
      res.status(202).json({ taskId: task.id });
    } catch (err) {
      log(`ERROR: /enqueue: ${String(err)}`);
      res.status(500).json({ error: "internal error" });
    }
  };
}
