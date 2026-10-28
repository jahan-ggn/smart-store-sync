import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import type { Prisma } from "@prisma/client";
import db from "../db.server";
import {
  checkImportShop,
  hashImportFile,
  IMPORT_BATCH_SIZE,
} from "./import-jobs.server";
import { createImportRules } from "./import-rules.server";
import { readSourceCsv } from "./source-csv.server";
import { createImageImporter } from "./shopify-images.server";
import type { SourceProduct } from "./source-csv.server";

type Admin = Parameters<typeof createImageImporter>[0];
type Snapshot = {
  settings: Parameters<typeof createImportRules>[0];
  mappings: Parameters<typeof createImportRules>[1];
};
const LEASE_MS = 120_000;

export async function imageProgress(shop: string, jobId: string) {
  checkImportShop(shop);
  const job = await db.importJob.findFirst({ where: { id: jobId, shop } });
  if (!job) throw new Error("Import job not found for this shop.");
  return {
    jobId,
    status: job.status,
    eligible: job.eligible,
    checked: job.imageProductsSynced,
    remaining: job.eligible - job.imageProductsSynced,
    lastImageRow: job.lastImageRow,
  };
}

// maxGroups = 0 processes all remaining products; otherwise stop after that many groups of 25.
export async function runImageImport(
  shop: string,
  jobId: string,
  admin: Admin,
  maxGroups = 1,
) {
  checkImportShop(shop);
  if (!Number.isSafeInteger(maxGroups) || maxGroups < 0)
    throw new Error("Invalid group limit.");
  const initial = await db.importJob.findFirst({
    where: { id: jobId, shop, activeShop: shop },
  });
  if (!initial) throw new Error("An active job for this shop was not found.");
  if (initial.status === "images_synced") return imageProgress(shop, jobId);
  const token = randomUUID();
  const claimed = await db.importJob.updateMany({
    where: {
      id: jobId,
      shop,
      activeShop: shop,
      OR: [
        { status: { in: ["inventory_synced", "images_ready"] } },
        { status: "images_running", leaseExpiresAt: { lte: new Date() } },
      ],
    },
    data: {
      status: "images_running",
      workerToken: token,
      leaseExpiresAt: new Date(Date.now() + LEASE_MS),
    },
  });
  if (claimed.count !== 1)
    throw new Error("Job is not ready or already has a live worker.");
  const owned = {
    id: jobId,
    shop,
    activeShop: shop,
    status: "images_running",
    workerToken: token,
  };
  let heartbeatError: Error | undefined;
  let heartbeat = Promise.resolve();
  let heartbeatBusy = false;
  let statusAfterRun = "images_ready";
  async function saveProgress(
    data: Prisma.ImportJobUpdateManyMutationInput = {},
  ) {
    if (heartbeatError) throw heartbeatError;
    const result = await db.importJob.updateMany({
      where: owned,
      data: { ...data, leaseExpiresAt: new Date(Date.now() + LEASE_MS) },
    });
    if (result.count !== 1) throw new Error("Worker ownership was lost.");
  }
  const timer = setInterval(() => {
    if (heartbeatBusy || heartbeatError) return;
    heartbeatBusy = true;
    heartbeat = saveProgress()
      .catch((error: unknown) => {
        heartbeatError =
          error instanceof Error
            ? error
            : new Error("Unable to renew worker ownership.");
      })
      .finally(() => {
        heartbeatBusy = false;
      });
  }, 20_000);
  try {
    // Read the current cursor after acquiring ownership, not before it.
    const job = await db.importJob.findFirstOrThrow({ where: owned });
    if (!job.sourcePath || !job.fileHash)
      throw new Error("Cancel the old job and prepare the original CSV again.");
    if ((await hashImportFile(job.sourcePath)) !== job.fileHash)
      throw new Error(
        "The retained CSV changed. Cancel this job and prepare a new copy.",
      );
    const snapshot = JSON.parse(job.ruleSnapshotJson) as Snapshot;
    const rules = createImportRules(snapshot.settings, snapshot.mappings);
    const importer = createImageImporter(admin);
    let rowNumber = 0;
    let synced = job.imageProductsSynced;
    let groups = 0;
    let stoppedAtLimit = false;
    let pending: SourceProduct[] = [];
    const flush = async () => {
      if (!pending.length) return;
      const startedAt = performance.now();
      const requestsBefore = importer.requestCount;
      for (let attempt = 1; attempt <= 3; attempt++) {
        try {
          await importer.syncGroup(pending, () => {
            if (heartbeatError) throw heartbeatError;
          });
          break;
        } catch (error) {
          if (heartbeatError) throw heartbeatError;
          if (attempt === 3) throw error;
          await delay(1000 * 2 ** attempt);
        }
      }
      const nextSynced = synced + pending.length;
      if (nextSynced > job.eligible)
        throw new Error("Image count exceeds the prepared product count.");
      await saveProgress({
        lastImageRow: rowNumber,
        imageProductsSynced: nextSynced,
        errorsJson: "[]",
      });
      synced = nextSynced;
      pending = [];
      groups++;
      console.log(
        `Images: ${synced}/${job.eligible} products checked. ` +
          `Group: ${((performance.now() - startedAt) / 1000).toFixed(1)}s, ` +
          `${importer.requestCount - requestsBefore} API requests.`,
      );
    };
    // Prepare one bounded window ahead, then checkpoint each group of 25.
    // Retain each product's CSV row so a restart resumes at the right place.
    let window: { product: SourceProduct; row: number }[] = [];
    const flushWindow = async () => {
      if (!window.length) return;
      for (let attempt = 1; attempt <= 3; attempt++) {
        try {
          await importer.prepareGroup(
            window.map((entry) => entry.product),
            () => {
              if (heartbeatError) throw heartbeatError;
            },
          );
          break;
        } catch (error) {
          if (heartbeatError) throw heartbeatError;
          if (attempt === 3) throw error;
          await delay(1000 * 2 ** attempt);
        }
      }
      console.log(
        `Image preparation: ${window.length} products checked ahead.`,
      );
      for (
        let offset = 0;
        offset < window.length;
        offset += IMPORT_BATCH_SIZE
      ) {
        const batch = window.slice(offset, offset + IMPORT_BATCH_SIZE);
        pending = batch.map((entry) => entry.product);
        rowNumber = batch[batch.length - 1].row;
        await flush();
      }
      window = [];
    };
    const windowSize = () =>
      Math.min(250, maxGroups ? (maxGroups - groups) * IMPORT_BATCH_SIZE : 250);
    let scannedRow = 0;
    for await (const row of readSourceCsv(job.sourcePath)) {
      scannedRow++;
      if (scannedRow <= job.lastImageRow) continue;
      if (!row.ok) throw new Error(`CSV row ${scannedRow}: ${row.error}`);
      if (rules.evaluate(row.product).eligible)
        window.push({ product: row.product, row: scannedRow });
      if (window.length === windowSize()) {
        await flushWindow();
        if (maxGroups && groups >= maxGroups) {
          stoppedAtLimit = true;
          break;
        }
      }
    }
    if (!stoppedAtLimit) {
      await flushWindow();
      if (synced !== job.eligible)
        throw new Error(
          "The CSV does not match its prepared eligible product count.",
        );
    }
    const finalStatus =
      synced === job.eligible ? "images_synced" : "images_ready";
    await saveProgress({
      status: finalStatus,
      ...(synced === job.eligible ? { lastImageRow: job.rowsRead } : {}),
      errorsJson: "[]",
    });
    statusAfterRun = finalStatus;
  } catch (error) {
    await db.importJob.updateMany({
      where: owned,
      data: {
        errorsJson: JSON.stringify([
          {
            error: (error instanceof Error
              ? error.message
              : "Import failed."
            ).slice(0, 2000),
          },
        ]),
      },
    });
    throw error;
  } finally {
    clearInterval(timer);
    await heartbeat;
    // A successful final save changed status but still belongs to this token.
    await db.importJob.updateMany({
      where: { id: jobId, shop, activeShop: shop, workerToken: token },
      data: { status: statusAfterRun, workerToken: null, leaseExpiresAt: null },
    });
  }
  return imageProgress(shop, jobId);
}
