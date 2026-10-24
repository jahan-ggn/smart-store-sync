-- CreateTable
CREATE TABLE "ImportJob" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shop" TEXT NOT NULL,
    "activeShop" TEXT,
    "status" TEXT NOT NULL DEFAULT 'preparing',
    "fileName" TEXT NOT NULL,
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
    "updatedAt" DATETIME NOT NULL
);

-- CreateTable
CREATE TABLE "ImportItem" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "jobId" TEXT NOT NULL,
    "sequence" INTEGER NOT NULL,
    "batchNumber" INTEGER NOT NULL,
    "storeId" INTEGER NOT NULL,
    "sourceProductId" TEXT NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'pending',
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "payloadJson" TEXT NOT NULL,
    "lastError" TEXT,
    "updatedAt" DATETIME NOT NULL,
    CONSTRAINT "ImportItem_jobId_fkey" FOREIGN KEY ("jobId") REFERENCES "ImportJob" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateIndex
CREATE UNIQUE INDEX "ImportJob_activeShop_key" ON "ImportJob"("activeShop");

-- CreateIndex
CREATE INDEX "ImportJob_shop_createdAt_idx" ON "ImportJob"("shop", "createdAt");

-- CreateIndex
CREATE INDEX "ImportItem_jobId_status_batchNumber_sequence_idx" ON "ImportItem"("jobId", "status", "batchNumber", "sequence");

-- CreateIndex
CREATE UNIQUE INDEX "ImportItem_jobId_storeId_sourceProductId_key" ON "ImportItem"("jobId", "storeId", "sourceProductId");
