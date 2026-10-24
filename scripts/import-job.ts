import process from "node:process";
import { resolve } from "node:path";
import db from "../app/db.server";
import {
  IMPORT_BATCH_SIZE,
  cancelPreparedImport,
  listImportJobs,
  prepareImportJob,
} from "../app/services/import-jobs.server";
async function main() {
  const [command, shop, path, ...extra] = process.argv.slice(2);
  if (
    !shop ||
    extra.length ||
    !["prepare", "status", "cancel"].includes(command ?? "") ||
    (command === "prepare" ? !path : Boolean(path))
  ) {
    throw new Error(
      "Usage: import-job.ts prepare <shop> <csv-path> | status <shop> | cancel <shop>",
    );
  }
  if (command === "prepare") {
    const job = await prepareImportJob(resolve(path!), shop);
    console.table({
      id: job.id,
      status: job.status,
      rowsRead: job.rowsRead,
      eligible: job.eligible,
      skipped: job.skipped,
      invalid: job.invalid,
      variants: job.variantCount,
      groupsOf25: Math.ceil(job.eligible / IMPORT_BATCH_SIZE),
      warnings: job.warnings,
    });
    console.log(
      "CSV retained on disk. Only job progress and the settings snapshot were saved to the database.",
    );
  } else if (command === "cancel") {
    await cancelPreparedImport(shop);
    console.log("Import cancelled and its retained CSV removed.");
  } else {
    console.table(
      (await listImportJobs(shop)).map((job) => ({
        id: job.id,
        status: job.status,
        eligible: job.eligible,
        active: job.activeShop !== null,
      })),
    );
  }
}
async function run() {
  try {
    await main();
  } catch (error) {
    console.error(
      error instanceof Error ? error.message : "Import job failed.",
    );
    process.exitCode = 1;
  } finally {
    await db.$disconnect();
  }
}
void run().catch(() => {
  console.error("Unable to close the database connection.");
  process.exitCode = 1;
});
