-- CreateTable
CREATE TABLE "CategoryMapping" (
    "shop" TEXT NOT NULL,
    "sourceStoreId" INTEGER NOT NULL,
    "sourceCategoryId" INTEGER NOT NULL,
    "collectionId" TEXT,
    "profitMargin" TEXT NOT NULL DEFAULT '0',
    "updatedAt" DATETIME NOT NULL,

    PRIMARY KEY ("shop", "sourceStoreId", "sourceCategoryId")
);
