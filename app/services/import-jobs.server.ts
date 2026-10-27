import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { copyFile, mkdir, rm, stat } from "node:fs/promises";
import { basename, join, resolve } from "node:path";
import { Prisma } from "@prisma/client";
import db from "../db.server";
import { loadImportRules } from "./import-rules.server";
import { readSourceCsv } from "./source-csv.server";
import { normalizeVariants } from "./variants.server";

export const IMPORT_BATCH_SIZE = 25;
const storage = resolve("var/imports");
export function checkImportShop(shop: string) {
  if (!/^[a-z0-9][a-z0-9-]*\.myshopify\.com$/.test(shop))
    throw new Error("Use a myshopify.com domain without a URL or path.");
}
export async function hashImportFile(path: string) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}
export async function cleanupImportHistory() {
  const eligible: Prisma.ImportJobWhereInput = {
    status: { in: ["failed", "cancelled", "completed"] },
    activeShop: null,
    workerToken: null,
    leaseExpiresAt: null,
    updatedAt: { lt: new Date(Date.now() - 7 * 86400_000) },
  };
  const old = await db.importJob.findMany({
    where: eligible,
    take: 100,
    select: { id: true, updatedAt: true },
  });
  for (const job of old) {
    try {
      await db.$transaction(async (tx) => {
        // The conditional delete both rechecks the snapshot and acquires the
        // database write lock before touching files. A competing cleaner sees
        // zero rows; an active or changed job keeps its files.
        const removed = await tx.importJob.deleteMany({
          where: { AND: [eligible, { id: job.id, updatedAt: job.updatedAt }] },
        });
        if (!removed.count) return;
        if (!/^[a-zA-Z0-9_-]+$/.test(job.id))
          throw new Error("Invalid retained import directory.");
        // Only app-managed copies are ours to remove, never an arbitrary
        // sourcePath. A filesystem failure rolls the database deletion back.
        await rm(join(storage, job.id), { recursive: true, force: true });
      });
    } catch (error) {
      // History retention is maintenance: keep processing imports and retry
      // this record on the next cleanup pass if disk access or locking fails.
      console.error(
        `Import history cleanup deferred for ${job.id}:`,
        error instanceof Error ? error.message : "Cleanup failed.",
      );
    }
  }
}
export class ImportJobConflict extends Error {}
export type SourceDelivery = {
  id: string;
  generatedAt: Date;
  storeIds: number[];
};
export async function prepareImportJob(
  filePath: string,
  shop: string,
  delivery?: SourceDelivery,
) {
  checkImportShop(shop);
  if (!(await stat(filePath)).isFile())
    throw new Error("The CSV path must be a file.");
  await cleanupImportHistory();
  const rules = await loadImportRules(shop);
  const job = await db
    .$transaction(async (tx) => {
      if (delivery) {
        const latest = await tx.importJob.findFirst({
          where: { shop, sourceGeneratedAt: { not: null } },
          orderBy: { sourceGeneratedAt: "desc" },
        });
        if (
          latest?.sourceGeneratedAt &&
          latest.sourceGeneratedAt >= delivery.generatedAt
        )
          throw new ImportJobConflict(
            "A newer or equal source delivery has already been accepted.",
          );
      }
      return tx.importJob.create({
        data: {
          shop,
          activeShop: shop,
          fileName: basename(filePath),
          ruleSnapshotJson: JSON.stringify(rules.snapshot),
          ...(delivery
            ? {
                sourceDeliveryId: delivery.id,
                sourceGeneratedAt: delivery.generatedAt,
              }
            : {}),
        },
      });
    })
    .catch((error: unknown) => {
      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === "P2002"
      )
        throw new ImportJobConflict(
          "This shop already has an active import. Finish or cancel it first.",
        );
      throw error;
    });
  const folder = join(storage, job.id);
  const sourcePath = join(folder, "source.csv");
  const counts = {
    rowsRead: 0,
    eligible: 0,
    skipped: 0,
    invalid: 0,
    variantCount: 0,
    warnings: 0,
  };
  const errors: { line?: number; error: string }[] = [];
  const sourceStoreIds = new Set<number>();
  try {
    await mkdir(folder, { recursive: true });
    await copyFile(filePath, sourcePath);
    const fileHash = await hashImportFile(sourcePath);
    for await (const row of readSourceCsv(sourcePath)) {
      counts.rowsRead++;
      if (!row.ok) {
        counts.invalid++;
        if (errors.length < 20)
          errors.push({ line: row.line, error: row.error });
        continue;
      }
      try {
        if (delivery && !delivery.storeIds.includes(row.product.storeId))
          throw new Error(
            `CSV store ${row.product.storeId} is outside this subscription.`,
          );
        sourceStoreIds.add(row.product.storeId);
        const variants = normalizeVariants(row.product);
        const decision = rules.evaluate(row.product);
        counts.warnings += row.warnings.length;
        if (!decision.eligible) {
          counts.skipped++;
          continue;
        }
        counts.eligible++;
        counts.variantCount += variants.variants.length;
      } catch (error) {
        counts.invalid++;
        if (errors.length < 20)
          errors.push({
            line: row.line,
            error: error instanceof Error ? error.message : "Invalid product.",
          });
      }
    }
    if (!counts.rowsRead) throw new Error("The CSV contains no product rows.");
    if (counts.invalid)
      throw new Error(
        "The CSV contains invalid products. Resolve them before importing.",
      );
    await db.$transaction(async (tx) => {
      const changed = await tx.importJob.updateMany({
        where: { id: job.id, shop, activeShop: shop, status: "preparing" },
        data: {
          ...counts,
          sourcePath,
          fileHash,
          status: "ready",
          errorsJson: "[]",
          ruleSnapshotJson: JSON.stringify({
            ...rules.snapshot,
            sourceStoreIds: delivery
              ? [...delivery.storeIds]
              : [...sourceStoreIds],
          }),
        },
      });
      if (changed.count !== 1) throw new Error("Preparation was cancelled.");
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Preparation failed.";
    if (errors.length < 20) errors.push({ error: message });
    await db.importJob.updateMany({
      where: { id: job.id, shop, activeShop: shop, status: "preparing" },
      data: {
        ...counts,
        status: "failed",
        activeShop: null,
        errorsJson: JSON.stringify(errors),
      },
    });
    await rm(folder, { recursive: true, force: true });
    throw new Error(`Job ${job.id}: ${message}`);
  }
  return db.importJob.findUniqueOrThrow({ where: { id: job.id } });
}
export async function listImportJobs(shop: string) {
  checkImportShop(shop);
  return db.importJob.findMany({
    where: { shop },
    orderBy: { createdAt: "desc" },
    take: 5,
  });
}
export async function cancelPreparedImport(shop: string) {
  checkImportShop(shop);
  const job = await db.importJob.findUnique({ where: { activeShop: shop } });
  if (!job) throw new Error("No active import was found.");
  const changed = await db.importJob.updateMany({
    where: {
      id: job.id,
      shop,
      activeShop: shop,
      OR: [
        {
          status: {
            in: [
              "preparing",
              "ready",
              "products_synced",
              "inventory_ready",
              "inventory_synced",
              "images_ready",
              "images_synced",
              "publishing_ready",
            ],
          },
        },
        {
          status: {
            in: [
              "running",
              "inventory_running",
              "images_running",
              "publishing_running",
            ],
          },
          leaseExpiresAt: { lte: new Date() },
        },
      ],
    },
    data: {
      status: "cancelled",
      activeShop: null,
      workerToken: null,
      leaseExpiresAt: null,
    },
  });
  if (changed.count !== 1)
    throw new Error(
      "A worker is still running. Stop it and wait two minutes before cancelling.",
    );
  if (job.sourcePath) await rm(job.sourcePath, { force: true });
  await rm(join(storage, job.id), { recursive: true, force: true });
}
