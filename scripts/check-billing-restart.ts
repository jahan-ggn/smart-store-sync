import process from "node:process";

async function main() {
  const [shop, ...extra] = process.argv.slice(2);

  if (
    !shop ||
    extra.length ||
    !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.myshopify\.com$/.test(shop)
  ) {
    console.error("Usage: check-billing-restart.ts <shop.myshopify.com>");
    process.exitCode = 1;
    return;
  }

  for (const name of [
    "DATABASE_URL",
    "SHOPIFY_API_KEY",
    "SHOPIFY_API_SECRET",
    "SHOPIFY_APP_URL",
    "SHOPIFY_PARTNER_ID",
    "SHOPIFY_PARTNER_API_TOKEN",
  ]) {
    if (!process.env[name]?.trim()) {
      console.error(`Missing configuration: ${name}`);
      process.exitCode = 1;
      return;
    }
  }

  const { default: db } = await import("../app/db.server");

  try {
    const { createImportAdmin } =
      await import("../app/services/import-admin.server");
    const { readShopifyBilling } =
      await import("../app/services/billing.server");

    const billing = await readShopifyBilling(shop, createImportAdmin(shop));

    console.log("Shop:", shop);
    console.log("Billing status:", billing.status);
    console.log("Approved until:", billing.expiresAt ?? "none");
    console.log("Cancels at cycle end:", billing.cancelAtEndOfCycle);
  } finally {
    await db.$disconnect();
  }
}

void main().catch(() => {
  // Never print raw exceptions, tokens or API response bodies.
  console.error(
    "Billing check failed. Check that .env matches Smart Store Sync Dev, " +
      "the Partner API client has Manage apps permission, and the app " +
      "has been opened in Shopify Admin through your local dev server.",
  );
  process.exitCode = 1;
});
