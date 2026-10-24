import { createHash } from "node:crypto";
import type { SourceProduct } from "./source-csv.server";

export type NormalizedVariant = {
  key: string;
  sku: string;
  optionValues: {
    name: string;
    value: string;
  }[];
  source: Record<string, unknown> | null;
};

export type NormalizedVariants = {
  options: {
    name: string;
    values: string[];
  }[];
  variants: NormalizedVariant[];
};

function scalar(value: unknown, label: string) {
  if (
    typeof value !== "string" &&
    !(typeof value === "number" && Number.isFinite(value))
  ) {
    throw new Error(`${label} must be text or a finite number.`);
  }

  const text = String(value).normalize("NFC").trim();

  if (!text) {
    throw new Error(`${label} cannot be empty.`);
  }

  return text;
}

function readSizeOption(source: Record<string, unknown>) {
  const name = scalar(source.name, "Option name").replace(/\s+/g, " ");

  const value = scalar(source.value, `Value for ${name}`);

  return new Map([[name.toLowerCase(), { name, value }]]);
}

function variant(
  product: SourceProduct,
  key: string,
  optionValues: NormalizedVariant["optionValues"],
  source: Record<string, unknown> | null,
): NormalizedVariant {
  const suppliedSku = source?.sku;

  const generatedSku =
    "SSS-" +
    createHash("sha256")
      .update(JSON.stringify([product.storeId, product.productId, key]))
      .digest("hex")
      .slice(0, 32);

  const sku =
    suppliedSku === undefined ||
    suppliedSku === null ||
    (typeof suppliedSku === "string" && !suppliedSku.trim())
      ? generatedSku
      : scalar(suppliedSku, "Variant SKU");

  return {
    key,
    sku,
    optionValues,
    source,
  };
}

export function normalizeVariants(product: SourceProduct): NormalizedVariants {
  if (!product.hasVariants) {
    if (product.variants.length) {
      throw new Error("A simple product cannot contain source variants.");
    }

    return {
      options: [],
      variants: [variant(product, "default", [], null)],
    };
  }

  if (product.variants.length === 0 || product.variants.length > 2048) {
    throw new Error(
      "A variable product must contain between 1 and 2048 variants.",
    );
  }

  const rows = product.variants.map(readSizeOption);

  // Attribute order must not change option positions or keys.
  const names = [...rows[0].keys()].sort();

  const optionNames = names.map((name) => rows[0].get(name)!.name);

  const values = names.map(() => new Set<string>());
  const seen = new Set<string>();
  const skus = new Set<string>();

  const variants = rows.map((row, index) => {
    if (row.size !== names.length || names.some((name) => !row.has(name))) {
      throw new Error(
        `Variant ${index + 1} has missing or inconsistent option names.`,
      );
    }

    const optionValues = names.map((name, position) => ({
      name: optionNames[position],
      value: row.get(name)!.value,
    }));

    const key = createHash("sha256")
      .update(JSON.stringify(names.map((name) => [name, row.get(name)!.value])))
      .digest("hex");

    if (seen.has(key)) {
      throw new Error(`Variant ${index + 1} repeats an option combination.`);
    }

    seen.add(key);

    optionValues.forEach((option, position) => {
      values[position].add(option.value);
    });

    const normalized = variant(
      product,
      key,
      optionValues,
      product.variants[index],
    );

    if (skus.has(normalized.sku)) {
      throw new Error(`Variant ${index + 1} repeats a SKU.`);
    }

    skus.add(normalized.sku);

    return normalized;
  });

  return {
    options: names.map((_, position) => ({
      name: optionNames[position],
      values: [...values[position]],
    })),
    variants,
  };
}
