import process from "node:process";
import db from "../app/db.server";
import { checkImportShop } from "../app/services/import-jobs.server";
import { runProductImport } from "../app/services/import-worker.server";
import { runInventoryImport } from "../app/services/inventory.server";

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
  checkImportShop(shop);
  for (const name of [
    "SHOPIFY_API_KEY",
    "SHOPIFY_API_SECRET",
    "SHOPIFY_APP_URL",
  ]) {
    if (!process.env[name]) throw new Error(`Set ${name} in .env first.`);
  }
  const job = await db.importJob.findFirstOrThrow({
    where: { id: jobId, shop, activeShop: shop },
  });
  const { unauthenticated } = await import("../app/shopify.server");
  const { admin } = await unauthenticated.admin(shop);
  if (
    [
      "products_synced",
      "inventory_ready",
      "inventory_running",
      "inventory_synced",
    ].includes(job.status)
  ) {
    console.table(
      await runInventoryImport(shop, jobId, admin, all ? 0 : 1, locationId),
    );
  } else {
    const progress = await runProductImport(shop, jobId, admin, all ? 0 : 1);
    console.table(progress);
    if (all && progress.status === "products_synced")
      console.table(
        await runInventoryImport(shop, jobId, admin, 0, locationId),
      );
  }
  console.log(
    "Products, collections and inventory are handled by this importer. Images and publishing are still pending.",
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
