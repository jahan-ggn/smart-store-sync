-- CreateTable
CREATE TABLE "AccountSettings" (
    "shop" TEXT NOT NULL PRIMARY KEY,
    "tokenEncrypted" TEXT NOT NULL,
    "defaultMargin" TEXT NOT NULL DEFAULT '0.00',
    "minPrice" TEXT NOT NULL DEFAULT '0.00',
    "maxPrice" TEXT NOT NULL DEFAULT '0.00',
    "skipUnmapped" BOOLEAN NOT NULL DEFAULT true,
    "updatedAt" DATETIME NOT NULL
);
