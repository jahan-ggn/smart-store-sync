/*
  Warnings:

  - You are about to drop the `ImportItem` table. If the table is not empty, all the data it contains will be lost.

*/
-- AlterTable
ALTER TABLE "ImportJob" ADD COLUMN "leaseExpiresAt" DATETIME;
ALTER TABLE "ImportJob" ADD COLUMN "sourcePath" TEXT;
ALTER TABLE "ImportJob" ADD COLUMN "workerToken" TEXT;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "ImportItem";
PRAGMA foreign_keys=on;

-- CreateTable
CREATE TABLE "ImportBatch" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "jobId" TEXT NOT NULL,
    "number" INTEGER NOT NULL,
    "startRow" INTEGER NOT NULL,
    "endRow" INTEGER NOT NULL,
    "productCount" INTEGER NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'pending',
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "lastError" TEXT,
    "updatedAt" DATETIME NOT NULL,
    CONSTRAINT "ImportBatch_jobId_fkey" FOREIGN KEY ("jobId") REFERENCES "ImportJob" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateIndex
CREATE INDEX "ImportBatch_jobId_status_number_idx" ON "ImportBatch"("jobId", "status", "number");

-- CreateIndex
CREATE UNIQUE INDEX "ImportBatch_jobId_number_key" ON "ImportBatch"("jobId", "number");
