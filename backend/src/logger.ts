import { appendFileSync } from "fs";

export function log(message: string): void {
  const line = `[${new Date().toISOString()}] ${message}\n`;
  appendFileSync("logs.txt", line);
  console.log(message);
}
