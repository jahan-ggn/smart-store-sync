-- RedefineTables
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;
CREATE TABLE "new_ImportJob" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shop" TEXT NOT NULL,
    "activeShop" TEXT,
    "status" TEXT NOT NULL DEFAULT 'preparing',
    "workerToken" TEXT,
    "leaseExpiresAt" DATETIME,
    "fileName" TEXT NOT NULL,
    "sourcePath" TEXT,
    "fileHash" TEXT,
    "ruleSnapshotJson" TEXT NOT NULL,
    "rowsRead" INTEGER NOT NULL DEFAULT 0,
    "eligible" INTEGER NOT NULL DEFAULT 0,
    "skipped" INTEGER NOT NULL DEFAULT 0,
    "invalid" INTEGER NOT NULL DEFAULT 0,
    "variantCount" INTEGER NOT NULL DEFAULT 0,
    "warnings" INTEGER NOT NULL DEFAULT 0,
    "errorsJson" TEXT NOT NULL DEFAULT '[]',
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" DATETIME NOT NULL,
    "lastCompletedRow" INTEGER NOT NULL DEFAULT 0,
    "productsSynced" INTEGER NOT NULL DEFAULT 0,
    "inventoryCursor" TEXT,
    "inventoryLocationId" TEXT,
    "inventoryProductsProcessed" INTEGER NOT NULL DEFAULT 0,
    "lastImageRow" INTEGER NOT NULL DEFAULT 0,
    "imageProductsSynced" INTEGER NOT NULL DEFAULT 0
);
INSERT INTO "new_ImportJob" ("activeShop", "createdAt", "eligible", "errorsJson", "fileHash", "fileName", "id", "invalid", "inventoryCursor", "inventoryLocationId", "inventoryProductsProcessed", "lastCompletedRow", "leaseExpiresAt", "productsSynced", "rowsRead", "ruleSnapshotJson", "shop", "skipped", "sourcePath", "status", "updatedAt", "variantCount", "warnings", "workerToken") SELECT "activeShop", "createdAt", "eligible", "errorsJson", "fileHash", "fileName", "id", "invalid", "inventoryCursor", "inventoryLocationId", "inventoryProductsProcessed", "lastCompletedRow", "leaseExpiresAt", "productsSynced", "rowsRead", "ruleSnapshotJson", "shop", "skipped", "sourcePath", "status", "updatedAt", "variantCount", "warnings", "workerToken" FROM "ImportJob";
DROP TABLE "ImportJob";
ALTER TABLE "new_ImportJob" RENAME TO "ImportJob";
CREATE UNIQUE INDEX "ImportJob_activeShop_key" ON "ImportJob"("activeShop");
CREATE INDEX "ImportJob_shop_createdAt_idx" ON "ImportJob"("shop", "createdAt");
PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;
