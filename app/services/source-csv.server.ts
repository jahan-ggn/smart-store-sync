import { createReadStream } from "node:fs";
import { parse } from "csv-parse";

const requiredColumns = [
  "store_id",
  "product_id",
  "product_name",
  "product_url",
  "image_url",
  "current_price",
  "original_price",
  "stock_status",
  "is_active",
  "has_variants",
  "variants",
  "categories",
];

type RawRow = Record<string, string>;

export type SourceProduct = {
  storeId: number;
  productId: string;
  name: string;
  productUrl: string;
  imageUrls: string[];
  currentPrice: string;
  originalPrice: string;
  active: boolean;
  inStock: boolean;
  categoryIds: number[];
  hasVariants: boolean;
  variants: Record<string, unknown>[];
  raw: RawRow;
};

export type SourceCsvResult =
  | {
      ok: true;
      line: number;
      product: SourceProduct;
      warnings: string[];
    }
  | {
      ok: false;
      line: number;
      error: string;
    };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function text(row: RawRow, field: string) {
  return (row[field] ?? "").trim();
}

function positiveId(value: string, label: string) {
  if (!/^[1-9]\d*$/.test(value)) {
    throw new Error(`${label} must be a positive integer.`);
  }

  return value;
}

function numericId(value: string, label: string) {
  positiveId(value, label);

  const id = Number(value);

  if (!Number.isSafeInteger(id) || id > 2147483647) {
    throw new Error(`${label} is outside the supported database ID range.`);
  }

  return id;
}

function money(value: string, label: string) {
  if (!/^\d{1,15}(?:\.\d{1,2})?$/.test(value)) {
    throw new Error(
      `${label} must be a non-negative amount with at most two decimal places.`,
    );
  }

  const [whole, fraction = ""] = value.split(".");

  return `${BigInt(whole)}.${fraction.padEnd(2, "0")}`;
}

function flag(value: string, label: string, yes: string[], no: string[]) {
  const normalized = value.toLowerCase();

  if (yes.includes(normalized)) return true;
  if (no.includes(normalized)) return false;

  throw new Error(`${label} has an unsupported value.`);
}

function url(value: string, label: string) {
  let parsed: URL;

  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`${label} must be an absolute HTTP or HTTPS URL.`);
  }

  if (
    !["http:", "https:"].includes(parsed.protocol) ||
    parsed.username ||
    parsed.password
  ) {
    throw new Error(
      `${label} must be an HTTP or HTTPS URL without credentials.`,
    );
  }

  return value;
}

function jsonArray(value: string, label: string): Record<string, unknown>[] {
  if (!value) return [];

  let parsed: unknown;

  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error(`${label} contains invalid JSON.`);
  }

  if (!Array.isArray(parsed) || !parsed.every(isRecord)) {
    throw new Error(`${label} must be a JSON array of objects.`);
  }

  return parsed;
}

function normalizeRow(raw: RawRow): SourceProduct {
  const storeId = numericId(text(raw, "store_id"), "store_id");

  const productId = positiveId(text(raw, "product_id"), "product_id");

  const name = text(raw, "product_name");

  if (!name) {
    throw new Error("product_name is required.");
  }

  const hasVariants = flag(
    text(raw, "has_variants"),
    "has_variants",
    ["1", "true", "yes", "y", "on"],
    ["0", "false", "no", "n", "off"],
  );

  const variants = jsonArray(text(raw, "variants"), "variants");

  if (hasVariants !== variants.length > 0) {
    throw new Error("has_variants does not match the variants array.");
  }

  const categories = text(raw, "categories");

  const categoryIds = categories
    ? [
        ...new Set(
          categories
            .split(",")
            .map((value) => numericId(value.trim(), "category ID")),
        ),
      ]
    : [];

  const gallery = text(raw, "product_images");

  const imageUrls = [
    ...new Set([
      url(text(raw, "image_url"), "image_url"),
      ...(gallery
        ? gallery
            .split(",")
            .map((value) => url(value.trim(), "gallery image URL"))
        : []),
    ]),
  ];

  return {
    storeId,
    productId,
    name,
    productUrl: url(text(raw, "product_url"), "product_url"),
    imageUrls,
    currentPrice: money(text(raw, "current_price"), "current_price"),
    originalPrice: money(text(raw, "original_price"), "original_price"),
    active: flag(
      text(raw, "is_active"),
      "is_active",
      ["1", "true", "yes", "active"],
      ["0", "false", "no", "inactive"],
    ),
    inStock: flag(
      text(raw, "stock_status"),
      "stock_status",
      ["instock", "in_stock", "available", "1", "true", "yes"],
      ["outofstock", "out_of_stock", "0", "false", "no"],
    ),
    categoryIds,
    hasVariants,
    variants,
    raw,
  };
}

export async function* readSourceCsv(
  filePath: string,
): AsyncGenerator<SourceCsvResult> {
  let headerFound = false;

  const parser = parse({
    bom: true,
    skip_empty_lines: true,
    max_record_size: 2_000_000,
    info: true,
    columns: (header: string[]) => {
      const columns = header.map((column) => column.trim().toLowerCase());

      if (
        columns.some((column) => !column) ||
        new Set(columns).size !== columns.length
      ) {
        throw new Error("CSV headers must be nonempty and unique.");
      }

      const missing = requiredColumns.filter(
        (column) => !columns.includes(column),
      );

      if (missing.length) {
        throw new Error(`Missing CSV columns: ${missing.join(", ")}`);
      }

      headerFound = true;

      return columns;
    },
  });

  const input = createReadStream(filePath);

  input.on("error", (error) => parser.destroy(error));
  input.pipe(parser);

  const seen = new Set<string>();

  try {
    for await (const item of parser) {
      const { record, info } = item as {
        record: RawRow;
        info: { lines: number };
      };

      let product: SourceProduct;

      try {
        product = normalizeRow(record);

        const key = `${product.storeId}:${product.productId}`;

        if (seen.has(key)) {
          throw new Error(`Duplicate source product: ${key}`);
        }

        seen.add(key);
      } catch (error) {
        yield {
          ok: false,
          line: info.lines,
          error: error instanceof Error ? error.message : "Invalid row.",
        };

        continue;
      }

      const warnings: string[] = [];

      if (
        BigInt(product.originalPrice.replace(".", "")) <
        BigInt(product.currentPrice.replace(".", ""))
      ) {
        warnings.push("original_price is below current_price.");
      }

      yield {
        ok: true,
        line: info.lines,
        product,
        warnings,
      };
    }

    if (!headerFound) {
      throw new Error("The CSV is empty or has no header.");
    }
  } finally {
    input.destroy();
    parser.destroy();
  }
}
