import process from "node:process";
import db from "../app/db.server";
import { createImportAdmin } from "../app/services/import-admin.server";
import { checkImportShop } from "../app/services/import-jobs.server";

const QUERY = `#graphql
  query ImportTokenRefreshCheck { shop { id } }
`;

async function main() {
  const [shop, flag, ...extra] = process.argv.slice(2);
  if (!shop || flag !== "--test-refresh" || extra.length)
    throw new Error(
      "Usage: check-import-auth.ts <shop.myshopify.com> --test-refresh",
    );
  checkImportShop(shop);
  if (process.env.NODE_ENV === "production")
    throw new Error("Run this diagnostic only in local development.");
  const active = await db.importJob.findUnique({ where: { activeShop: shop } });
  if (active)
    throw new Error(
      "Finish the shop's active import before testing token refresh.",
    );
  const admin = createImportAdmin(shop);
  const check = async () => {
    const response = await admin.graphql(QUERY, {
      signal: AbortSignal.timeout(60_000),
    });
    const result = (await response.json()) as {
      errors?: unknown[];
      data?: { shop?: { id?: string } };
    };
    if (!response.ok || result.errors?.length || !result.data?.shop?.id)
      throw new Error("The read-only Shopify authentication check failed.");
  };
  // Keep this client instance across the expiry simulation, just as a long job does.
  await check();
  const before = await db.session.findUnique({
    where: { id: `offline_${shop}` },
  });
  if (!before || before.isOnline || !before.expires || !before.refreshToken)
    throw new Error(
      "An expiring offline session with refresh credentials is required. Open the app in Shopify Admin first.",
    );
  if (
    before.refreshTokenExpires &&
    before.refreshTokenExpires.getTime() <= Date.now()
  )
    throw new Error(
      "The refresh token has expired. Open the app in Shopify Admin to reconnect.",
    );
  const simulatedExpiry = new Date(Date.now() + 120_000);
  // Change only the local expiry metadata. Never restore a rotated credential pair.
  const changed = await db.session.updateMany({
    where: {
      id: before.id,
      accessToken: before.accessToken,
      expires: before.expires,
    },
    data: { expires: simulatedExpiry },
  });
  if (changed.count !== 1)
    throw new Error(
      "The session changed during setup. Stop other app processes and retry.",
    );
  try {
    // allSettled ensures no request is left running when restoring metadata or disconnecting.
    const results = await Promise.allSettled(Array.from({ length: 5 }, check));
    const failure = results.find((result) => result.status === "rejected");
    if (failure?.status === "rejected") throw failure.reason;
    const after = await db.session.findUniqueOrThrow({
      where: { id: before.id },
    });
    const tokenChanged = after.accessToken !== before.accessToken;
    const refreshTokenChanged = Boolean(
      after.refreshToken && after.refreshToken !== before.refreshToken,
    );
    const expiryExtended = Boolean(
      after.expires && after.expires.getTime() > Date.now() + 300_000,
    );
    if (!tokenChanged || !refreshTokenChanged || !expiryExtended)
      throw new Error(
        "The API responded, but the expected refreshed credentials were not saved.",
      );
    await check();
    console.table({
      concurrentRequestsSucceeded: 5,
      tokenChanged,
      refreshTokenChanged,
      expiryExtended,
      followUpRequestSucceeded: true,
    });
    console.log(
      "PASS: the same import client continued through token refresh. No products were changed; no credentials were printed.",
    );
  } finally {
    // Restore only our simulated expiry if the original access token is still stored.
    // A successful SDK refresh already saved a new token/expiry and is left intact.
    await db.session.updateMany({
      where: {
        id: before.id,
        accessToken: before.accessToken,
        expires: simulatedExpiry,
      },
      data: { expires: before.expires },
    });
  }
}

async function run() {
  try {
    await main();
  } catch (error) {
    console.error(
      error instanceof Error
        ? error.message
        : "Token refresh check failed. Reopen the app in Shopify Admin and retry.",
    );
    process.exitCode = 1;
  } finally {
    await db.$disconnect();
  }
}
void run().catch(() => {
  console.error("Unable to close the database connection.");
  process.exitCode = 1;
});
