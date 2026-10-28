import { readdir, rm, stat } from "node:fs/promises";
import { resolve, join } from "node:path";
import db from "../db.server";
import { cleanupImportHistory } from "./import-jobs.server";
import { runImportStages } from "./import-runner.server";
import {
  requireSourceSubscription,
  SourceSubscriptionError,
} from "./source-subscription.server";

const READY = [
  "ready",
  "products_synced",
  "inventory_ready",
  "inventory_synced",
  "images_ready",
  "images_synced",
  "publishing_ready",
];
const RUNNING = [
  "running",
  "inventory_running",
  "images_running",
  "publishing_running",
];

export async function maintainAutomaticImports() {
  // A request or host crash can leave preparation unfinished. This stage never
  // writes to Shopify, and automatic requests are limited to a 60-second upload.
  await db.importJob.updateMany({
    where: {
      sourceDeliveryId: { not: null },
      status: "preparing",
      updatedAt: { lt: new Date(Date.now() - 10 * 60_000) },
    },
    data: {
      status: "failed",
      activeShop: null,
      errorsJson: JSON.stringify([
        { error: "CSV preparation was interrupted. Send a fresh delivery." },
      ]),
    },
  });
  await cleanupImportHistory();
  const incoming = resolve("var/imports/incoming");
  const folders = await readdir(incoming, { withFileTypes: true }).catch(
    () => [],
  );
  for (const folder of folders) {
    if (!folder.isDirectory() || !folder.name.startsWith("upload-")) continue;
    const path = join(incoming, folder.name);
    const info = await stat(path).catch(() => null);
    if (info && info.mtimeMs < Date.now() - 86400_000)
      await rm(path, { recursive: true, force: true });
  }
}
export async function runNextAutomaticImport() {
  const now = new Date();
  const job = await db.importJob.findFirst({
    where: {
      sourceDeliveryId: { not: null },
      activeShop: { not: null },
      AND: [
        { OR: [{ nextAttemptAt: null }, { nextAttemptAt: { lte: now } }] },
        {
          OR: [
            { status: { in: READY } },
            { status: { in: RUNNING }, leaseExpiresAt: { lte: now } },
          ],
        },
      ],
    },
    orderBy: { updatedAt: "asc" },
  });
  if (!job) return false;
  if (RUNNING.includes(job.status)) {
    const recovered = await db.importJob.updateMany({
      where: {
        id: job.id,
        workerToken: job.workerToken,
        status: job.status,
        leaseExpiresAt: { lte: now },
      },
      data: {
        status:
          job.status === "running"
            ? "ready"
            : job.status.replace("_running", "_ready"),
        workerToken: null,
        leaseExpiresAt: null,
      },
    });
    if (!recovered.count) return true;
  }
  try {
    const subscription = await requireSourceSubscription(job.shop);
    const snapshot = JSON.parse(job.ruleSnapshotJson) as {
      sourceStoreIds?: number[];
    };
    if (
      !Array.isArray(snapshot.sourceStoreIds) ||
      !snapshot.sourceStoreIds.length ||
      snapshot.sourceStoreIds.some((id) => !subscription.storeIds.includes(id))
    )
      throw new SourceSubscriptionError(
        "The subscribed source stores changed. Send a fresh CSV for the current selection.",
        403,
      );
    // Prepare up to 250 image products, with checkpoints every 25, then yield.
    console.table(await runImportStages(job.shop, job.id, false, undefined, 10));
    await db.importJob.updateMany({
      where: { id: job.id, workerToken: null },
      data: { automaticFailures: 0, nextAttemptAt: null },
    });
  } catch (error) {
    const permanent =
      error instanceof SourceSubscriptionError && error.status === 403;
    const attempts = job.automaticFailures + 1;
    const failed = permanent || attempts >= 5;
    const current = await db.importJob.findUnique({
      where: { id: job.id },
      select: { errorsJson: true },
    });
    const changed = await db.importJob.updateMany({
      // Never alter another live worker's ownership after a claim race.
      where: {
        id: job.id,
        automaticFailures: job.automaticFailures,
        activeShop: job.shop,
        workerToken: null,
        status: { in: READY },
      },
      data: {
        automaticFailures: attempts,
        nextAttemptAt: failed
          ? null
          : new Date(
              Date.now() + Math.min(15 * 60_000, 30_000 * 2 ** (attempts - 1)),
            ),
        ...(failed ? { status: "failed", activeShop: null } : {}),
        errorsJson:
          !permanent && current?.errorsJson && current.errorsJson !== "[]"
            ? current.errorsJson
            : JSON.stringify([
                {
                  error: permanent
                    ? error.message
                    : "Automatic import failed. Check app credentials, source availability and the worker logs, then resume or send a fresh delivery.",
                },
              ]),
      },
    });
    if (changed.count)
      console.error(
        `Automatic import ${job.id}: ${failed ? "failed" : "retry scheduled"}.`,
      );
  }
  return true;
}
