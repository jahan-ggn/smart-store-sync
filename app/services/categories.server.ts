import db from "../db.server";
import type { authenticate } from "../shopify.server";
import { loadStores } from "./stores.server";

export class CategoryMappingError extends Error {}

type Admin = Awaited<ReturnType<typeof authenticate.admin>>["admin"];

type Collection = {
  id: string;
  title: string;
};

type CollectionsResponse = {
  errors?: { message: string }[];
  data?: {
    collections: {
      nodes: Collection[];
      pageInfo: {
        hasNextPage: boolean;
        endCursor: string | null;
      };
    };
  };
};

async function loadCollections(admin: Admin) {
  const collections: Collection[] = [];
  let after: string | null = null;

  do {
    const response = await admin.graphql(
      `#graphql
        query MappingCollections($after: String) {
          collections(first: 100, after: $after, sortKey: TITLE) {
            nodes {
              id
              title
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
      `,
      { variables: { after } },
    );

    const result = (await response.json()) as CollectionsResponse;

    if (!response.ok || result.errors?.length || !result.data?.collections) {
      throw new CategoryMappingError(
        "Unable to load Shopify collections. Please try again.",
      );
    }

    const page = result.data.collections;

    collections.push(...page.nodes);

    if (!page.pageInfo.hasNextPage) break;

    if (!page.pageInfo.endCursor || page.pageInfo.endCursor === after) {
      throw new CategoryMappingError("Unable to load all Shopify collections.");
    }

    after = page.pageInfo.endCursor;
  } while (after);

  return collections;
}

export async function loadCategoryMapping(shop: string, admin: Admin) {
  const source = await loadStores(shop);

  const stores = source.stores.filter((store) =>
    source.selectedIds.includes(store.id),
  );

  const [collections, saved] = await Promise.all([
    stores.length ? loadCollections(admin) : Promise.resolve([]),
    db.categoryMapping.findMany({
      where: { shop },
    }),
  ]);

  return {
    collections,
    stores: stores.map((store) => ({
      id: store.id,
      name: store.name,
      rows: store.categories.map((category) => {
        const mapping = saved.find(
          (item) =>
            item.sourceStoreId === store.id &&
            item.sourceCategoryId === category.id,
        );

        return {
          categoryId: category.id,
          categoryName: category.name,
          collectionId: mapping?.collectionId ?? "",
          profitMargin: mapping?.profitMargin ?? "0",
        };
      }),
    })),
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

export async function saveCategoryMapping(
  shop: string,
  admin: Admin,
  form: FormData,
) {
  const storeId = Number(form.get("store_id"));
  const raw = form.get("mappings");

  if (
    !Number.isSafeInteger(storeId) ||
    storeId <= 0 ||
    typeof raw !== "string"
  ) {
    throw new CategoryMappingError("Invalid category mapping submission.");
  }

  let submitted: unknown;

  try {
    submitted = JSON.parse(raw);
  } catch {
    throw new CategoryMappingError("Invalid category mapping submission.");
  }

  if (!Array.isArray(submitted)) {
    throw new CategoryMappingError("Invalid category mapping submission.");
  }

  // Recheck enabled stores, categories and this shop's collections.
  const current = await loadCategoryMapping(shop, admin);

  const store = current.stores.find((item) => item.id === storeId);

  if (!store) {
    throw new CategoryMappingError(
      "This source store is no longer enabled. Reload the page.",
    );
  }

  const categoryIds = new Set(store.rows.map((row) => row.categoryId));

  const collectionIds = new Set(
    current.collections.map((collection) => collection.id),
  );

  const seen = new Set<number>();

  const values = submitted.map((row) => {
    if (
      !isRecord(row) ||
      typeof row.categoryId !== "number" ||
      !categoryIds.has(row.categoryId) ||
      seen.has(row.categoryId)
    ) {
      throw new CategoryMappingError(
        "The category list has changed. Reload the page before saving.",
      );
    }

    seen.add(row.categoryId);

    if (
      typeof row.collectionId !== "string" ||
      (row.collectionId !== "" && !collectionIds.has(row.collectionId))
    ) {
      throw new CategoryMappingError(
        "A selected collection is unavailable. Reload and choose another collection.",
      );
    }

    if (
      typeof row.profitMargin !== "string" ||
      !/^\d{1,10}$/.test(row.profitMargin)
    ) {
      throw new CategoryMappingError(
        "Every category margin must be a non-negative whole number, up to 10 digits.",
      );
    }

    return {
      sourceCategoryId: row.categoryId,
      collectionId: row.collectionId || null,
      profitMargin: BigInt(row.profitMargin).toString(),
    };
  });

  if (seen.size !== categoryIds.size) {
    throw new CategoryMappingError(
      "The category list has changed. Reload the page before saving.",
    );
  }

  await db.$transaction(
    values.map((value) =>
      db.categoryMapping.upsert({
        where: {
          shop_sourceStoreId_sourceCategoryId: {
            shop,
            sourceStoreId: storeId,
            sourceCategoryId: value.sourceCategoryId,
          },
        },
        create: {
          shop,
          sourceStoreId: storeId,
          ...value,
        },
        update: {
          collectionId: value.collectionId,
          profitMargin: value.profitMargin,
        },
      }),
    ),
  );
}
