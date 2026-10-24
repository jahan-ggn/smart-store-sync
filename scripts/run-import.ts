import process from "node:process";
import db from "../app/db.server";
import { checkImportShop } from "../app/services/import-jobs.server";
import { runProductImport } from "../app/services/import-worker.server";
async function main() {
  const [shop, jobId, mode, ...extra] = process.argv.slice(2);
  if (
    !shop ||
    !jobId ||
    extra.length ||
    (mode !== undefined && mode !== "--all")
  )
    throw new Error("Usage: run-import.ts <shop> <job-id> [--all]");
  checkImportShop(shop);
  for (const name of [
    "SHOPIFY_API_KEY",
    "SHOPIFY_API_SECRET",
    "SHOPIFY_APP_URL",
  ]) {
    if (!process.env[name]) throw new Error(`Set ${name} in .env first.`);
  }
  const { unauthenticated } = await import("../app/shopify.server");
  const { admin } = await unauthenticated.admin(shop);
  const progress = await runProductImport(
    shop,
    jobId,
    admin,
    mode === "--all" ? 0 : 1,
  );
  console.table(progress);

  console.log(
    "Product and collection sync only. Images, inventory and publishing are still pending.",
  );
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
