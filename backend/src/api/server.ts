import { createRedisClient } from "../redisClient.js";
import { PORT, SHUTDOWN_TIMEOUT_MS } from "../config.js";
import { log } from "../logger.js";
import { createShutdownSignal } from "../shutdown.js";
import { isMainModule } from "../entrypoint.js";
import { createApp } from "./app.js";

async function main(): Promise<void> {
  const client = createRedisClient();
  const app = createApp(client);

  const server = app.listen(PORT, () => {
    log(`API server listening on port ${PORT}`);
  });

  const signal = createShutdownSignal();
  signal.addEventListener("abort", () => {
    log("API server shutting down...");
    const forceExitTimer = setTimeout(() => {
      log("API server shutdown timed out waiting for connections to drain, forcing exit");
      process.exit(1);
    }, SHUTDOWN_TIMEOUT_MS);
    server.close(() => {
      clearTimeout(forceExitTimer);
      client.quit().catch(console.error).finally(() => process.exit(0));
    });
  });
}

if (isMainModule(import.meta.url)) {
  main().catch(console.error);
}
