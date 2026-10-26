import process from "node:process";
import db from "../app/db.server";
import { checkImportShop } from "../app/services/import-jobs.server";
import { createImportAdmin } from "../app/services/import-admin.server";
import { runProductImport } from "../app/services/import-worker.server";
import { runInventoryImport } from "../app/services/inventory.server";
import { runImageImport } from "../app/services/image-worker.server";
import {
  publishingProgress,
  runPublishingImport,
} from "../app/services/publishing.server";

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
  const job = await db.importJob.findFirstOrThrow({
    where: { id: jobId, shop },
  });
  if (job.status === "completed") {
    console.table(await publishingProgress(shop, jobId));
    console.log("Import already completed. No Shopify changes were made.");
    return;
  }
  if (job.activeShop !== shop)
    throw new Error("This import is no longer active. Prepare a fresh import.");
  for (const name of [
    "SHOPIFY_API_KEY",
    "SHOPIFY_API_SECRET",
    "SHOPIFY_APP_URL",
  ]) {
    if (!process.env[name]) throw new Error(`Set ${name} in .env first.`);
  }
  const admin = createImportAdmin(shop);
  const finishPublishing = async () => {
    console.table(await runPublishingImport(shop, jobId, admin, all ? 0 : 1));
  };
  const finishImages = async () => {
    const progress = await runImageImport(shop, jobId, admin, all ? 0 : 1);
    console.table(progress);
    if (all && progress.status === "images_synced") await finishPublishing();
  };
  const finishInventory = async () => {
    const progress = await runInventoryImport(
      shop,
      jobId,
      admin,
      all ? 0 : 1,
      locationId,
    );
    console.table(progress);
    if (all && progress.status === "inventory_synced") await finishImages();
  };
  if (
    ["images_synced", "publishing_ready", "publishing_running"].includes(
      job.status,
    )
  ) {
    await finishPublishing();
  } else if (
    ["inventory_synced", "images_ready", "images_running"].includes(job.status)
  ) {
    await finishImages();
  } else if (
    ["products_synced", "inventory_ready", "inventory_running"].includes(
      job.status,
    )
  ) {
    await finishInventory();
  } else {
    const progress = await runProductImport(shop, jobId, admin, all ? 0 : 1);
    console.table(progress);
    if (all && progress.status === "products_synced") await finishInventory();
  }

  console.log(
    "Products, collections, inventory, images and Online Store publishing are handled by this importer. Final status is completed when all stages finish.",
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
