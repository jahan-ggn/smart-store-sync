import process from "node:process";
import db from "../app/db.server";
import { runImportStages } from "../app/services/import-runner.server";

async function main() {
  const [shop, jobId, ...flags] = process.argv.slice(2);
  let all = false;
  let locationId: string | undefined;
  for (let index = 0; index < flags.length; index++) {
    if (flags[index] === "--all" && !all) all = true;
    else if (flags[index] === "--location" && !locationId && flags[index + 1])
      locationId = flags[++index];
    else
      throw new Error(
        "Usage: run-import.ts <shop> <job-id> [--all] [--location gid://shopify/Location/ID]",
      );
  }
  if (!shop || !jobId)
    throw new Error("Supply a shop domain and import job ID.");
  console.table(await runImportStages(shop, jobId, all, locationId));
}
async function run() {
  try {
    await main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : "Import failed.");
    process.exitCode = 1;
  } finally {
    await db.$disconnect();
  }
}
void run().catch(() => {
  console.error("Unable to close the database connection.");
  process.exitCode = 1;
});
