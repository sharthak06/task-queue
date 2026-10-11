import type { Redis } from "ioredis";
import type { RequestHandler } from "express";
import { log } from "../../logger.js";

export function createTasksHandler(client: Redis): RequestHandler {
  return async (req, res) => {
    try {
      const id = req.params["id"];
      if (id === undefined) {
        res.status(400).json({ error: "task id is required" });
        return;
      }

      const hash = await client.hgetall(`task:${id}`);
      if (Object.keys(hash).length === 0) {
        res.status(404).json({ error: "task not found" });
        return;
      }

      res.status(200).json(hash);
    } catch (err) {
      log(`ERROR: /tasks/:id: ${String(err)}`);
      res.status(500).json({ error: "internal error" });
    }
  };
}
