import process from "node:process";
import { setTimeout as delay } from "node:timers/promises";
import db from "../app/db.server";
import {
  maintainAutomaticImports,
  runNextAutomaticImport,
} from "../app/services/automatic-import.server";

let stopping = false;
process.once("SIGINT", () => {
  stopping = true;
});
process.once("SIGTERM", () => {
  stopping = true;
});
async function run() {
  const once = process.argv.slice(2);
  if (once.length && (once.length !== 1 || once[0] !== "--once"))
    throw new Error("Usage: import-worker.ts [--once]");
  let maintainedAt = 0;
  while (!stopping) {
    if (Date.now() - maintainedAt > 60_000) {
      await maintainAutomaticImports();
      maintainedAt = Date.now();
    }
    const worked = await runNextAutomaticImport();
    if (once.length) break;
    if (!stopping) await delay(worked ? 250 : 2000);
  }
}
void run()
  .catch((error: unknown) => {
    console.error(
      "Import worker failed:",
      error instanceof Error ? error.message : "Unknown error.",
    );
    process.exitCode = 1;
  })
  .finally(() => db.$disconnect());
