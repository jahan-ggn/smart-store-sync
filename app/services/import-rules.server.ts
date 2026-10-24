import db from "../db.server";
import type { SourceProduct } from "./source-csv.server";

type Settings = {
  defaultMargin: string;
  minPrice: string;
  maxPrice: string;
  skipUnmapped: boolean;
};

type Mapping = {
  sourceStoreId: number;
  sourceCategoryId: number;
  collectionId: string | null;
  profitMargin: string;
};

export type SkipReason =
  "inactive" | "out_of_stock" | "unmapped" | "below_minimum" | "above_maximum";

export type ImportDecision =
  | {
      eligible: false;
      reason: SkipReason;
    }
  | {
      eligible: true;
      sourcePrice: string;
      appliedMargin: string;
      sellingPrice: string;
      compareAtPrice: string | null;
      collectionIds: string[];
    };

function minorUnits(value: string, label: string) {
  if (!/^\d{1,15}(?:\.\d{1,2})?$/.test(value)) {
    throw new Error(`${label} is not a valid non-negative amount.`);
  }

  const [whole, fraction = ""] = value.split(".");

  return BigInt(whole) * 100n + BigInt(fraction.padEnd(2, "0"));
}

function wholeAmount(value: string, label: string) {
  const amount = minorUnits(value, label);

  if (amount % 100n !== 0n) {
    throw new Error(`${label} must be a whole number.`);
  }

  return amount;
}

function formatMoney(value: bigint) {
  return `${value / 100n}.${(value % 100n).toString().padStart(2, "0")}`;
}

export function createImportRules(settings: Settings, mappings: Mapping[]) {
  const defaultMargin = wholeAmount(settings.defaultMargin, "Default margin");

  const minimum = wholeAmount(settings.minPrice, "Minimum source price");

  const maximum = wholeAmount(settings.maxPrice, "Maximum source price");

  if (maximum !== 0n && minimum > maximum) {
    throw new Error(
      "Maximum source price must be at least the minimum, or 0 to disable it.",
    );
  }

  const byCategory = new Map(
    mappings.map((mapping) => [
      `${mapping.sourceStoreId}:${mapping.sourceCategoryId}`,
      {
        collectionId: mapping.collectionId,
        margin: wholeAmount(mapping.profitMargin, "Category margin"),
      },
    ]),
  );

  function evaluate(product: SourceProduct): ImportDecision {
    if (!product.active) {
      return { eligible: false, reason: "inactive" };
    }

    if (!product.inStock) {
      return { eligible: false, reason: "out_of_stock" };
    }

    let margin = defaultMargin;
    const collections = new Set<string>();

    for (const categoryId of product.categoryIds) {
      const mapping = byCategory.get(`${product.storeId}:${categoryId}`);

      if (!mapping) continue;

      if (mapping.collectionId) {
        collections.add(mapping.collectionId);
      }

      if (mapping.margin > margin) {
        margin = mapping.margin;
      }
    }

    if (settings.skipUnmapped && collections.size === 0) {
      return { eligible: false, reason: "unmapped" };
    }

    const sourcePrice = minorUnits(product.currentPrice, "Source price");

    if (minimum !== 0n && sourcePrice < minimum) {
      return {
        eligible: false,
        reason: "below_minimum",
      };
    }

    if (maximum !== 0n && sourcePrice > maximum) {
      return {
        eligible: false,
        reason: "above_maximum",
      };
    }

    const sellingPrice = sourcePrice + margin;

    const originalPrice = minorUnits(product.originalPrice, "Original price");

    return {
      eligible: true,
      sourcePrice: formatMoney(sourcePrice),
      appliedMargin: formatMoney(margin),
      sellingPrice: formatMoney(sellingPrice),
      compareAtPrice:
        originalPrice > sellingPrice ? formatMoney(originalPrice) : null,
      collectionIds: [...collections],
    };
  }

  return {
    settings: { ...settings },
    mappingCount: mappings.length,
    snapshot: {
      settings: { ...settings },
      mappings: mappings.map((mapping) => ({ ...mapping })),
    },
    evaluate,
  };
}

export async function loadImportRules(shop: string) {
  if (!/^[a-z0-9][a-z0-9-]*\.myshopify\.com$/.test(shop)) {
    throw new Error(
      "Use the shop's myshopify.com domain without https:// or a path.",
    );
  }

  const [settings, mappings] = await db.$transaction([
    db.accountSettings.findUnique({
      where: { shop },
      select: {
        defaultMargin: true,
        minPrice: true,
        maxPrice: true,
        skipUnmapped: true,
      },
    }),

    db.categoryMapping.findMany({
      where: { shop },
      select: {
        sourceStoreId: true,
        sourceCategoryId: true,
        collectionId: true,
        profitMargin: true,
      },
    }),
  ]);

  if (!settings) {
    throw new Error(`No saved Account & Plan settings found for ${shop}.`);
  }

  return createImportRules(settings, mappings);
}
