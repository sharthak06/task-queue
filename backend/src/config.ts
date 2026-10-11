export const BASE_DELAY_MS = 1_000;
export const MAX_ATTEMPTS = 5;
export const VISIBILITY_TIMEOUT_MS = 30_000;
export const POLL_INTERVAL_MS = 500;
export const REAPER_INTERVAL_MS = 10_000;
export const TASK_STATUS_TTL_SECONDS = 24 * 60 * 60;
export const PORT = Number(process.env["PORT"] ?? 3000);
export const SHUTDOWN_TIMEOUT_MS = 5_000;
