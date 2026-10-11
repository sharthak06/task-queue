import express from "express";
import type { Express, NextFunction, Request, Response } from "express";
import type { Redis } from "ioredis";
import { createEnqueueHandler } from "./routes/enqueue.js";
import { createTasksHandler } from "./routes/tasks.js";
import { createDlqHandler } from "./routes/dlq.js";
import { createHealthHandler } from "./routes/health.js";
import { createMetricsHandler } from "./routes/metrics.js";

export function createApp(client: Redis): Express {
  const app = express();

  app.use(express.json());
  app.use((err: unknown, _req: Request, res: Response, next: NextFunction) => {
    if (err instanceof SyntaxError && "body" in err) {
      res.status(400).json({ error: "invalid JSON in request body" });
      return;
    }
    next(err);
  });

  app.post("/enqueue", createEnqueueHandler(client));
  app.get("/tasks/:id", createTasksHandler(client));
  app.get("/dlq", createDlqHandler(client));
  app.get("/health", createHealthHandler(client));
  app.get("/metrics", createMetricsHandler(client));

  return app;
}
