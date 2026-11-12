import db from "../db.server";
import process from "node:process";
import { createImportAdmin } from "./import-admin.server";
import { checkImportShop } from "./import-jobs.server";
import { runProductImport } from "./import-worker.server";
import { runInventoryImport } from "./inventory.server";
import { runImageImport } from "./image-worker.server";
import { publishingProgress, runPublishingImport } from "./publishing.server";
import {
  requireSourceSubscription,
  SourceSelectionChanged,
} from "./source-subscription.server";

// Shared by the CLI and automatic worker. Every batch rechecks source access.
export async function runImportStages(
  shop: string,
  jobId: string,
  all = false,
  locationId?: string,
) {
  let progress = await runImportBatch(shop, jobId, locationId);

  while (all && progress.status !== "completed") {
    progress = await runImportBatch(shop, jobId, locationId);
  }

  return progress;
}

async function runImportBatch(
  shop: string,
  jobId: string,
  locationId?: string,
) {
  checkImportShop(shop);

  const job = await db.importJob.findFirstOrThrow({
    where: { id: jobId, shop },
  });

  if (job.status === "completed") {
    return publishingProgress(shop, jobId);
  }

  if (job.activeShop !== shop) {
    throw new Error("This import is no longer active.");
  }

  const subscription = await requireSourceSubscription(shop);
  const snapshot = JSON.parse(job.ruleSnapshotJson) as {
    sourceStoreIds?: unknown;
  };

  if (
    !Array.isArray(snapshot.sourceStoreIds) ||
    !snapshot.sourceStoreIds.length ||
    snapshot.sourceStoreIds.some(
      (id: unknown) =>
        typeof id !== "number" ||
        !Number.isSafeInteger(id) ||
        id < 1 ||
        !subscription.storeIds.includes(id),
    )
  ) {
    throw new SourceSelectionChanged();
  }

  for (const name of [
    "SHOPIFY_API_KEY",
    "SHOPIFY_API_SECRET",
    "SHOPIFY_APP_URL",
  ]) {
    if (!process.env[name]) {
      throw new Error(`Set ${name} in the server environment first.`);
    }
  }

  const admin = createImportAdmin(shop);

  if (
    ["images_synced", "publishing_ready", "publishing_running"].includes(
      job.status,
    )
  ) {
    return runPublishingImport(shop, jobId, admin, 1);
  }

  if (
    ["inventory_synced", "images_ready", "images_running"].includes(job.status)
  ) {
    return runImageImport(shop, jobId, admin, 1);
  }

  if (
    ["products_synced", "inventory_ready", "inventory_running"].includes(
      job.status,
    )
  ) {
    return runInventoryImport(shop, jobId, admin, 1, locationId);
  }

  return runProductImport(shop, jobId, admin, 1);
}
