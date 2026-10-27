import { rm } from "node:fs/promises";
import { join, resolve } from "node:path";
import db from "../db.server";
import { checkImportShop } from "./import-jobs.server";

// Revoke credentials and worker ownership even when Shopify no longer supplies
// a session (for example, a repeated uninstall or shop/redact delivery).
export async function stopShopImports(shop: string) {
  checkImportShop(shop);
  await db.$transaction([
    db.session.deleteMany({ where: { shop } }),
    db.importJob.updateMany({
      where: { shop, activeShop: shop },
      data: {
        status: "cancelled",
        activeShop: null,
        workerToken: null,
        leaseExpiresAt: null,
        nextAttemptAt: null,
      },
    }),
  ]);
}

export async function redactShopData(shop: string) {
  await stopShopImports(shop);
  // Removing the account also prevents fresh source deliveries from starting.
  await db.$transaction([
    db.accountSettings.deleteMany({ where: { shop } }),
    db.categoryMapping.deleteMany({ where: { shop } }),
  ]);
  const jobs = await db.importJob.findMany({
    where: { shop },
    select: { id: true },
  });
  for (const job of jobs) {
    // Never trust sourcePath as a recursive deletion target. Retained imports
    // belong to the app's own directory, identified by the database job ID.
    if (!/^[a-zA-Z0-9_-]+$/.test(job.id)) {
      throw new Error("Invalid retained import directory.");
    }
    await rm(join(resolve("var/imports"), job.id), {
      recursive: true,
      force: true,
    });
    // Delete only after filesystem cleanup succeeds, so retries can finish it.
    await db.importJob.deleteMany({ where: { id: job.id, shop } });
  }
}
