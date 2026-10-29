import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import type { ImportJob } from "@prisma/client";
import db from "../db.server";
import { checkImportShop, hashImportFile } from "./import-jobs.server";
import { createImportRules } from "./import-rules.server";
import { readSourceCsv } from "./source-csv.server";
import { normalizeVariants } from "./variants.server";
import { createRequester } from "./shopify-product.server";

type Admin = Parameters<typeof createRequester>[0];
type Request = ReturnType<typeof createRequester>["request"];
type PageInfo = { hasNextPage: boolean; endCursor: string | null };
type Level = { quantities: { name: string; quantity: number }[] };
type Variant = {
  id: string;
  sku: string | null;
  inventoryPolicy: string;
  inventoryItem: { id: string; tracked: boolean; inventoryLevel: Level | null };
};
type VariantPage = { nodes: Variant[]; pageInfo: PageInfo };
type Product = {
  id: string;
  sourceId: { jsonValue: unknown } | null;
  variants: VariantPage;
};
type UserError = { field: string[] | null; message: string; code?: string };

export class InventoryLocationError extends Error {}

const VARIANTS = `
  nodes {
    id sku inventoryPolicy
    inventoryItem {
      id tracked
      inventoryLevel(locationId: $locationId) {
        quantities(names: ["available"]) { name quantity }
      }
    }
  }
  pageInfo { hasNextPage endCursor }
`;
const SCAN = `#graphql
  query InventoryProducts($after: String, $locationId: ID!) {
    products(first: 25, after: $after, sortKey: ID) {
      nodes {
        id
        sourceId: metafield(namespace: "$app", key: "source_product_id") { jsonValue }
        variants(first: 10) { ${VARIANTS} }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
`;
const MORE = `#graphql
  query InventoryMoreVariants($id: ID!, $after: String!, $locationId: ID!) {
    product(id: $id) {
      id
      variants(first: 100, after: $after) { ${VARIANTS} }
    }
  }
`;
const TRACK = `#graphql
  mutation InventoryTrackVariants($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
    productVariantsBulkUpdate(productId: $productId, variants: $variants, allowPartialUpdates: false) {
      productVariants { id }
      userErrors { field message }
    }
  }
`;
const ACTIVATE = `#graphql
  mutation InventoryActivateItem($inventoryItemId: ID!, $locationId: ID!, $available: Int!, $idempotencyKey: String!) {
    inventoryActivate(inventoryItemId: $inventoryItemId, locationId: $locationId, available: $available) @idempotent(key: $idempotencyKey) {
      inventoryLevel { quantities(names: ["available"]) { name quantity } }
      userErrors { field message }
    }
  }
`;
const SET = `#graphql
  mutation InventorySetAvailable($input: InventorySetQuantitiesInput!, $idempotencyKey: String!) {
    inventorySetQuantities(input: $input) @idempotent(key: $idempotencyKey) {
      inventoryAdjustmentGroup { createdAt }
      userErrors { field message code }
    }
  }
`;
const LOCATIONS = `#graphql
  query InventoryLocations($after: String) {
    locations(first: 100, after: $after) {
      nodes { id name isActive fulfillsOnlineOrders }
      pageInfo { hasNextPage endCursor }
    }
  }
`;
const PRIMARY_LOCATION = `#graphql
  query InventoryPrimaryLocation {
    location { id name isActive fulfillsOnlineOrders }
  }
`;
function assertNoErrors(errors: UserError[]) {
  if (errors.length)
    throw new Error(
      errors
        .map((error) => `${error.code ?? "inventory"}: ${error.message}`)
        .join("; "),
    );
}
function available(level: Level) {
  const quantity = level.quantities.find(
    (value) => value.name === "available",
  )?.quantity;
  if (!Number.isSafeInteger(quantity))
    throw new Error("Shopify returned an invalid inventory quantity.");
  return quantity!;
}

// This map exists only in worker memory, never as per-product database records.
export async function loadInventoryFeed(job: ImportJob) {
  if (
    !job.sourcePath ||
    !job.fileHash ||
    (await hashImportFile(job.sourcePath)) !== job.fileHash
  )
    throw new Error(
      "The retained CSV is missing or changed. Prepare a new import.",
    );
  const snapshot = JSON.parse(job.ruleSnapshotJson) as {
    settings: Parameters<typeof createImportRules>[0];
    mappings: Parameters<typeof createImportRules>[1];
    sourceStoreIds?: number[];
  };
  const rules = createImportRules(snapshot.settings, snapshot.mappings);
  const stores = new Set<number>(snapshot.sourceStoreIds ?? []);
  const eligible = new Map<string, Set<string>>();
  let rows = 0;
  for await (const row of readSourceCsv(job.sourcePath)) {
    rows++;
    if (!row.ok) throw new Error(`Invalid CSV row ${rows}: ${row.error}`);
    const variants = normalizeVariants(row.product);
    stores.add(row.product.storeId);
    if (rules.evaluate(row.product).eligible)
      eligible.set(
        `${row.product.storeId}:${row.product.productId}`,
        new Set(variants.variants.map((variant) => variant.sku)),
      );
  }
  const signedEmptySnapshot =
    Boolean(job.sourceDeliveryId && job.sourceGeneratedAt) &&
    stores.size > 0 &&
    [...stores].every((id) => Number.isSafeInteger(id) && id > 0);
  if (
    (!rows && !signedEmptySnapshot) ||
    rows !== job.rowsRead ||
    eligible.size !== job.eligible
  )
    throw new Error("The retained CSV does not match the prepared import.");
  return { stores, eligible };
}
async function chooseLocation(request: Request, supplied?: string) {
  if (!supplied) {
    // With no ID, Shopify returns the shop's configured primary location.
    const { location } = await request<{
      location: {
        id: string;
        name: string;
        isActive: boolean;
        fulfillsOnlineOrders: boolean;
      } | null;
    }>(PRIMARY_LOCATION, {}, 10);
    if (!location?.isActive || !location.fulfillsOnlineOrders)
      throw new InventoryLocationError(
        "The shop's primary location is unavailable or does not fulfill online orders. Update the default location in Shopify Settings > Locations.",
      );
    return location.id;
  }
  const locations: {
    id: string;
    name: string;
    isActive: boolean;
    fulfillsOnlineOrders: boolean;
  }[] = [];
  let after: string | null = null;
  const seen = new Set<string>();
  do {
    const data: { locations: { nodes: typeof locations; pageInfo: PageInfo } } =
      await request(LOCATIONS, { after }, 150);
    locations.push(
      ...data.locations.nodes.filter(
        (location) => location.isActive && location.fulfillsOnlineOrders,
      ),
    );
    if (!data.locations.pageInfo.hasNextPage) break;
    after = data.locations.pageInfo.endCursor;
    if (!after || seen.has(after))
      throw new Error("Invalid Shopify location cursor.");
    seen.add(after);
  } while (after);
  if (!locations.some((location) => location.id === supplied))
    throw new InventoryLocationError(
      "The selected location is unavailable or does not fulfill online orders.",
    );
  return supplied;
}

// Discover Shopify's primary location on the first automatic delivery.
// Keep the saved choice on subsequent deliveries and during concurrent requests.
export async function ensureAutomaticInventoryLocation(
  shop: string,
  admin: Admin,
) {
  checkImportShop(shop);
  const account = await db.accountSettings.findUnique({
    where: { shop },
    select: { inventoryLocationId: true },
  });
  if (!account)
    throw new InventoryLocationError("Save Account & Plan settings first.");
  if (account.inventoryLocationId) return account.inventoryLocationId;

  const api = createRequester(admin);
  const locationId = await chooseLocation(api.request);
  await db.accountSettings.updateMany({
    where: { shop, inventoryLocationId: null },
    data: { inventoryLocationId: locationId },
  });
  const saved = await db.accountSettings.findUniqueOrThrow({
    where: { shop },
    select: { inventoryLocationId: true },
  });
  if (!saved.inventoryLocationId)
    throw new Error("Unable to save the automatic inventory location.");
  return saved.inventoryLocationId;
}

export async function inventoryProgress(shop: string, jobId: string) {
  checkImportShop(shop);
  const job = await db.importJob.findFirstOrThrow({
    where: { id: jobId, shop },
  });
  return {
    jobId: job.id,
    status: job.status,
    productsSynced: job.productsSynced,
    inventoryProductsProcessed: job.inventoryProductsProcessed,
    inventoryLocationId: job.inventoryLocationId,
  };
}

export async function runInventoryImport(
  shop: string,
  jobId: string,
  admin: Admin,
  maxPages = 1,
  suppliedLocation?: string,
) {
  checkImportShop(shop);
  if (!Number.isSafeInteger(maxPages) || maxPages < 0)
    throw new Error("Invalid inventory page limit.");
  const initial = await db.importJob.findFirstOrThrow({
    where: { id: jobId, shop, activeShop: shop },
  });
  if (initial.status === "inventory_synced")
    return inventoryProgress(shop, jobId);
  if (initial.productsSynced !== initial.eligible)
    throw new Error(
      "Finish product and collection sync before inventory sync.",
    );
  if (
    initial.inventoryLocationId &&
    suppliedLocation &&
    initial.inventoryLocationId !== suppliedLocation
  )
    throw new Error(
      "An inventory job cannot switch locations midway. Finish or cancel it first.",
    );
  const token = randomUUID();
  const claim = await db.importJob.updateMany({
    where: {
      id: jobId,
      shop,
      activeShop: shop,
      OR: [
        { status: { in: ["products_synced", "inventory_ready"] } },
        { status: "inventory_running", leaseExpiresAt: { lte: new Date() } },
      ],
    },
    data: {
      status: "inventory_running",
      workerToken: token,
      leaseExpiresAt: new Date(Date.now() + 120_000),
    },
  });
  if (claim.count !== 1)
    throw new Error("Inventory is not ready or another worker is active.");
  const owned = {
    id: jobId,
    shop,
    activeShop: shop,
    workerToken: token,
    status: "inventory_running",
  };
  let heartbeatError: Error | undefined;
  let heartbeat = Promise.resolve();
  let heartbeatBusy = false;
  let finalStatus = "inventory_ready";
  const assertOwned = () => {
    if (heartbeatError) throw heartbeatError;
  };
  const checkpoint = async (
    data: Parameters<typeof db.importJob.updateMany>[0]["data"] = {},
  ) => {
    assertOwned();
    const changed = await db.importJob.updateMany({
      where: owned,
      data: { ...data, leaseExpiresAt: new Date(Date.now() + 120_000) },
    });
    if (changed.count !== 1)
      throw new Error("Inventory worker ownership was lost.");
  };
  const timer = setInterval(() => {
    if (heartbeatBusy || heartbeatError) return;
    heartbeatBusy = true;
    heartbeat = checkpoint()
      .catch((error: unknown) => {
        heartbeatError =
          error instanceof Error
            ? error
            : new Error("Inventory heartbeat failed.");
      })
      .finally(() => {
        heartbeatBusy = false;
      });
  }, 20_000);
  const api = createRequester(admin);
  const request: Request = (query, variables, estimate = 20) =>
    api.request(query, variables, estimate, assertOwned);
  try {
    const job = await db.importJob.findFirstOrThrow({ where: owned });
    const feed = await loadInventoryFeed(job);
    const settings = await db.accountSettings.findUniqueOrThrow({
      where: { shop },
    });
    const locationId = await chooseLocation(
      request,
      job.inventoryLocationId ??
        suppliedLocation ??
        settings.inventoryLocationId ??
        undefined,
    );
    await checkpoint({ inventoryLocationId: locationId });
    await db.accountSettings.update({
      where: { shop },
      data: { inventoryLocationId: locationId },
    });
    let after = job.inventoryCursor;
    let processed = job.inventoryProductsProcessed;
    let pages = 0;
    let done = false;
    while (!done && (!maxPages || pages < maxPages)) {
      const startedAt = performance.now();
      const requestsBefore = api.requestCount;
      let result: { pageInfo: PageInfo; processed: number } | undefined;
      // Re-read quantities on every retry, including after an ambiguous response.
      for (let attempt = 1; attempt <= 3; attempt++) {
        try {
          const data = await request<{
            products: { nodes: Product[]; pageInfo: PageInfo };
          }>(SCAN, { after, locationId }, 800);
          let pageProcessed = 0;
          const changes: {
            inventoryItemId: string;
            locationId: string;
            quantity: number;
            changeFromQuantity: number;
          }[] = [];

          const syncProduct = async (product: Product) => {
            const sourceId = product.sourceId?.jsonValue;
            if (
              typeof sourceId !== "string" ||
              !/^[1-9]\d*:[1-9]\d*$/.test(sourceId)
            )
              return;
            if (!feed.stores.has(Number(sourceId.split(":")[0]))) return;
            const variants = [...product.variants.nodes];
            let page = product.variants;
            const cursors = new Set<string>();
            while (page.pageInfo.hasNextPage) {
              const cursor = page.pageInfo.endCursor;
              if (!cursor || cursors.has(cursor))
                throw new Error("Invalid inventory variant cursor.");
              cursors.add(cursor);
              const more = await request<{
                product: { id: string; variants: VariantPage } | null;
              }>(MORE, { id: product.id, after: cursor, locationId }, 800);
              if (!more.product || more.product.id !== product.id)
                throw new Error("Product disappeared during inventory sync.");
              page = more.product.variants;
              variants.push(...page.nodes);
            }
            const expected = feed.eligible.get(sourceId);
            if (
              expected &&
              (variants.length !== expected.size ||
                variants.some(
                  (variant) => !variant.sku || !expected.has(variant.sku),
                ))
            )
              throw new Error(
                `Variants changed for ${sourceId}. Prepare a fresh product import before syncing inventory.`,
              );
            const target = expected ? 1000 : 0;
            const tracking = variants.filter(
              (variant) =>
                !variant.inventoryItem.tracked ||
                variant.inventoryPolicy !== "DENY",
            );
            for (let offset = 0; offset < tracking.length; offset += 250) {
              const input = tracking.slice(offset, offset + 250);
              const tracked = await request<{
                productVariantsBulkUpdate: {
                  productVariants: { id: string }[] | null;
                  userErrors: UserError[];
                };
              }>(
                TRACK,
                {
                  productId: product.id,
                  variants: input.map((variant) => ({
                    id: variant.id,
                    inventoryPolicy: "DENY",
                    inventoryItem: { tracked: true },
                  })),
                },
                20,
              );
              assertNoErrors(tracked.productVariantsBulkUpdate.userErrors);
              if (
                tracked.productVariantsBulkUpdate.productVariants?.length !==
                input.length
              )
                throw new Error("Shopify did not confirm inventory tracking.");
            }
            for (const variant of variants) {
              const item = variant.inventoryItem;
              let level = item.inventoryLevel;
              if (!level) {
                const activated = await request<{
                  inventoryActivate: {
                    inventoryLevel: Level | null;
                    userErrors: UserError[];
                  };
                }>(
                  ACTIVATE,
                  {
                    inventoryItemId: item.id,
                    locationId,
                    available: target,
                    idempotencyKey: randomUUID(),
                  },
                  20,
                );
                assertNoErrors(activated.inventoryActivate.userErrors);
                level = activated.inventoryActivate.inventoryLevel;
                if (!level)
                  throw new Error(
                    "Shopify did not activate the inventory item.",
                  );
              }
              const current = available(level);
              if (current !== target)
                changes.push({
                  inventoryItemId: item.id,
                  locationId,
                  quantity: target,
                  changeFromQuantity: current,
                });
            }
            pageProcessed++;
          };
          let nextProduct = 0;
          let firstError: unknown;
          const syncNext = async () => {
            while (!firstError && nextProduct < data.products.nodes.length) {
              const product = data.products.nodes[nextProduct++];
              try {
                await syncProduct(product);
              } catch (error) {
                firstError ??= error;
              }
            }
          };
          await Promise.all(
            Array.from(
              { length: Math.min(3, data.products.nodes.length) },
              () => syncNext(),
            ),
          );
          if (firstError) throw firstError;
          for (let offset = 0; offset < changes.length; offset += 250) {
            const changed = await request<{
              inventorySetQuantities: {
                inventoryAdjustmentGroup: { createdAt: string } | null;
                userErrors: UserError[];
              };
            }>(
              SET,
              {
                idempotencyKey: randomUUID(),
                input: {
                  name: "available",
                  reason: "correction",
                  referenceDocumentUri: `gid://smart-store-sync/ImportJob/${jobId}`,
                  quantities: changes.slice(offset, offset + 250),
                },
              },
              20,
            );
            assertNoErrors(changed.inventorySetQuantities.userErrors);
          }
          result = {
            pageInfo: data.products.pageInfo,
            processed: pageProcessed,
          };
          break;
        } catch (error) {
          assertOwned();
          if (attempt === 3) throw error;
          await delay(1000 * 2 ** attempt);
        }
      }
      if (!result) throw new Error("Inventory page did not complete.");
      if (
        result.pageInfo.hasNextPage &&
        (!result.pageInfo.endCursor || result.pageInfo.endCursor === after)
      )
        throw new Error("Invalid Shopify product cursor.");
      processed += result.processed;
      after = result.pageInfo.endCursor;
      done = !result.pageInfo.hasNextPage;
      await checkpoint({
        inventoryCursor: after,
        inventoryProductsProcessed: processed,
        errorsJson: "[]",
      });
      pages++;
      console.log(
        `Inventory: ${processed} imported products checked at ${locationId}. ` +
          `Group: ${((performance.now() - startedAt) / 1000).toFixed(1)}s, ` +
          `${api.requestCount - requestsBefore} API requests.`,
      );
    }
    await checkpoint({ errorsJson: "[]" });
    finalStatus = done ? "inventory_synced" : "inventory_ready";
  } catch (error) {
    await db.importJob.updateMany({
      where: owned,
      data: {
        errorsJson: JSON.stringify([
          {
            error: (error instanceof Error
              ? error.message
              : "Inventory sync failed."
            ).slice(0, 2000),
          },
        ]),
      },
    });
    throw error;
  } finally {
    clearInterval(timer);
    await heartbeat;
    await db.importJob.updateMany({
      where: owned,
      data: { status: finalStatus, workerToken: null, leaseExpiresAt: null },
    });
  }
  return inventoryProgress(shop, jobId);
}
