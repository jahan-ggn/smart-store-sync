-- CreateTable
CREATE TABLE "ShopLifecycle" (
    "shop" TEXT NOT NULL PRIMARY KEY,
    "authenticatedAt" DATETIME,
    "billingCheckedAt" DATETIME,
    "uninstalledAt" DATETIME,
    "revocationTokenEncrypted" TEXT,
    "revocationNextAttemptAt" DATETIME
);

-- CreateIndex
CREATE INDEX "ShopLifecycle_revocationNextAttemptAt_idx" ON "ShopLifecycle"("revocationNextAttemptAt");
