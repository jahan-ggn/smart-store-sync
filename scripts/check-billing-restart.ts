import process from "node:process";

async function main() {
  const [shop, mode, ...extra] = process.argv.slice(2);
  const synchronize = mode === "--sync";

  if (
    !shop ||
    extra.length ||
    (mode !== undefined && !synchronize) ||
    !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.myshopify\.com$/.test(shop)
  ) {
    console.error(
      "Usage: check-billing-restart.ts <shop.myshopify.com> [--sync]",
    );
    process.exitCode = 1;
    return;
  }

  const required = [
    "DATABASE_URL",
    "SHOPIFY_API_KEY",
    "SHOPIFY_API_SECRET",
    "SHOPIFY_APP_URL",
    "SHOPIFY_PARTNER_ID",
    "SHOPIFY_PARTNER_API_TOKEN",
  ];

  if (synchronize) {
    required.push(
      "SOURCE_API_BASE_URL",
      "SHOPIFY_PROVISIONING_API_KEY",
      "SOURCE_TOKEN_ENCRYPTION_KEY",
    );
  }

  for (const name of required) {
    if (!process.env[name]?.trim()) {
      console.error(`Missing configuration: ${name}`);
      process.exitCode = 1;
      return;
    }
  }

  if (synchronize) {
    const source = new URL(process.env.SOURCE_API_BASE_URL!);

    if (!["localhost", "127.0.0.1", "[::1]"].includes(source.hostname)) {
      console.error(
        "For --sync, point SOURCE_API_BASE_URL at your local development backend.",
      );
      process.exitCode = 1;
      return;
    }
  }

  const { default: db } = await import("../app/db.server");

  try {
    const { createImportAdmin } =
      await import("../app/services/import-admin.server");
    const { readShopifyBilling, syncShopifyBilling } =
      await import("../app/services/billing.server");

    const admin = createImportAdmin(shop);

    const billing = synchronize
      ? await syncShopifyBilling(shop, admin)
      : await readShopifyBilling(shop, admin);

    console.log("Shop:", shop);
    console.log("Billing status:", billing.status);
    console.log("Approved until:", billing.expiresAt ?? "none");
    console.log("Cancels at cycle end:", billing.cancelAtEndOfCycle);

    if (synchronize) {
      console.log("Backend expiry synchronized: true");
    }
  } finally {
    await db.$disconnect();
  }
}

void main().catch(() => {
  // Never print raw exceptions, tokens or API response bodies.
  console.error(
    "Billing check failed. Check that .env matches Smart Store Sync Dev, " +
      "the Partner API client has Manage apps permission, and the app " +
      "has been opened in Shopify Admin through your local dev server. " +
      "For --sync, also check the local backend, provisioning key and encryption key.",
  );
  process.exitCode = 1;
});
