import db from "../db.server";
import process from "node:process";
import { createImportAdmin } from "./import-admin.server";
import { checkImportShop } from "./import-jobs.server";
import { runProductImport } from "./import-worker.server";
import { runInventoryImport } from "./inventory.server";
import { runImageImport } from "./image-worker.server";
import { publishingProgress, runPublishingImport } from "./publishing.server";

// Shared by the CLI and the automatic worker; no detached HTTP background tasks.
export async function runImportStages(
  shop: string,
  jobId: string,
  all = false,
  locationId?: string,
) {
  checkImportShop(shop);
  const job = await db.importJob.findFirstOrThrow({
    where: { id: jobId, shop },
  });
  if (job.status === "completed") return publishingProgress(shop, jobId);
  if (job.activeShop !== shop)
    throw new Error("This import is no longer active.");
  for (const name of [
    "SHOPIFY_API_KEY",
    "SHOPIFY_API_SECRET",
    "SHOPIFY_APP_URL",
  ])
    if (!process.env[name])
      throw new Error(`Set ${name} in the server environment first.`);
  const admin = createImportAdmin(shop);
  const publish = () => runPublishingImport(shop, jobId, admin, all ? 0 : 1);
  const images = async () => {
    const progress = await runImageImport(shop, jobId, admin, all ? 0 : 1);
    return all && progress.status === "images_synced" ? publish() : progress;
  };
  const inventory = async () => {
    const progress = await runInventoryImport(
      shop,
      jobId,
      admin,
      all ? 0 : 1,
      locationId,
    );
    return all && progress.status === "inventory_synced" ? images() : progress;
  };
  if (
    ["images_synced", "publishing_ready", "publishing_running"].includes(
      job.status,
    )
  )
    return publish();
  if (
    ["inventory_synced", "images_ready", "images_running"].includes(job.status)
  )
    return images();
  if (
    ["products_synced", "inventory_ready", "inventory_running"].includes(
      job.status,
    )
  )
    return inventory();
  const progress = await runProductImport(shop, jobId, admin, all ? 0 : 1);
  return all && progress.status === "products_synced" ? inventory() : progress;
}
