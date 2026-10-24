import process from "node:process";
import { resolve } from "node:path";
import db from "../app/db.server";
import { normalizeVariants, type NormalizedVariants } from "../app/services/variants.server";
import { readSourceCsv } from "../app/services/source-csv.server";
import { loadImportRules, type SkipReason } from "../app/services/import-rules.server";

async function main() {
  const [filePath, suppliedShop, ...extra] = process.argv.slice(2);
  if (!filePath || !suppliedShop || extra.length) {
    throw new Error('Usage: node --env-file=.env --import tsx scripts/check-source-csv.ts "/absolute/path/products.csv" shop.myshopify.com');
  }

  const shop = suppliedShop.trim().toLowerCase();
  const rules = await loadImportRules(shop);
  console.log(`Dry run for ${shop}`);
  console.table({ ...rules.settings, mappingRows: rules.mappingCount });

  const totals = { rows: 0, valid: 0, invalid: 0, simple: 0, variable: 0, variantEntries: 0, warnings: 0, normalizedVariants: 0, eligibleVariants: 0, eligible: 0, skipped: 0 };
  const reasons: Record<SkipReason, number> = { inactive: 0, out_of_stock: 0, unmapped: 0, below_minimum: 0, above_maximum: 0 };
  const stores = new Map<number, { storeId: number; valid: number; eligible: number; skipped: number }>();
  const errors: { line: number; message: string }[] = [];
  const prices: { storeId: number; productId: string; sourcePrice: string; margin: string; sellingPrice: string; compareAtPrice: string; collections: string }[] = [];
  const skippedExamples: { line: number; storeId: number; productId: string; sourcePrice: string; reason: SkipReason }[] = [];

  const variantExamples: { storeId: number; productId: string; kind: string; sku: string; options: string }[] = [];
  const exampleKinds = new Set<string>();

  for await (const result of readSourceCsv(resolve(filePath))) {
    totals.rows++;
    if (!result.ok) {
      totals.invalid++;
      if (errors.length < 20) errors.push({ line: result.line, message: result.error });
      continue;
    }
    const product = result.product;
    let normalized: NormalizedVariants;
    try {
      normalized = normalizeVariants(product);
    } catch (error) {
      totals.invalid++;
      if (errors.length < 20) errors.push({ line: result.line, message: error instanceof Error ? error.message : "Invalid variants." });
      continue;
    }
    totals.valid++;
    totals.normalizedVariants += normalized.variants.length;
    if (product.hasVariants) totals.variable++;
    else totals.simple++;
    totals.variantEntries += product.variants.length;
    totals.warnings += result.warnings.length;

    const store = stores.get(product.storeId) ?? { storeId: product.storeId, valid: 0, eligible: 0, skipped: 0 };
    store.valid++;
    stores.set(product.storeId, store);
    const decision = rules.evaluate(product);
    if (!decision.eligible) {
      totals.skipped++;
      store.skipped++;
      reasons[decision.reason]++;
      if (skippedExamples.length < 10) skippedExamples.push({ line: result.line, storeId: product.storeId, productId: product.productId, sourcePrice: product.currentPrice, reason: decision.reason });
      continue;
    }
    totals.eligible++;
    totals.eligibleVariants += normalized.variants.length;
    store.eligible++;
    const kind = product.hasVariants ? "variable" : "simple";
    if (!exampleKinds.has(kind)) {
      exampleKinds.add(kind);
      for (const variant of normalized.variants.slice(0, 5)) {
        variantExamples.push({
          storeId: product.storeId,
          productId: product.productId,
          kind,
          sku: variant.sku,
          options: variant.optionValues.map((option) => `${option.name}=${option.value}`).join(" | ") || "(default)",
        });
      }
    }
    if (prices.length < 10) prices.push({ storeId: product.storeId, productId: product.productId, sourcePrice: decision.sourcePrice, margin: decision.appliedMargin, sellingPrice: decision.sellingPrice, compareAtPrice: decision.compareAtPrice ?? "", collections: decision.collectionIds.join(", ") });
  }

  if (totals.rows === 0) throw new Error("The CSV contains no product rows.");
  console.log("Summary");
  console.table(totals);
  console.log("Skip reasons (first matching reason per product)");
  console.table(reasons);
  console.log("Source stores");
  console.table([...stores.values()].sort((a, b) => a.storeId - b.storeId));
  if (variantExamples.length) { console.log("Normalized variant examples"); console.table(variantExamples); }
  if (prices.length) { console.log("Eligible product examples"); console.table(prices); }
  if (skippedExamples.length) { console.log("Skipped product examples"); console.table(skippedExamples); }
  if (errors.length) { console.log("Source validation errors"); console.table(errors); }
  console.log(totals.invalid ? "CSV contains invalid rows. Resolve them before importing." : "Dry run complete. No database records or Shopify products were changed.");
  process.exitCode = totals.invalid ? 1 : 0;
}

async function run() {
  try {
    await main();
  } catch (error) {
    console.error("CSV dry run failed:", error instanceof Error ? error.message : "Unknown error.");
    process.exitCode = 1;
  } finally {
    await db.$disconnect();
  }
}

void run().catch(() => {
  console.error("Unable to close the database connection.");
  process.exitCode = 1;
});
