import process from "node:process";
import { createHash } from "node:crypto";
import { copyFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import db from "../app/db.server";
import { createImportAdmin } from "../app/services/import-admin.server";
import { createRequester } from "../app/services/shopify-product.server";
import { createProductPublisher } from "../app/services/publishing.server";
import {
  checkImportShop,
  hashImportFile,
} from "../app/services/import-jobs.server";
import { loadImportRules } from "../app/services/import-rules.server";
import {
  readSourceCsv,
  type SourceProduct,
} from "../app/services/source-csv.server";
import { normalizeVariants } from "../app/services/variants.server";
import { sourceImageFilename } from "../app/services/shopify-images.server";

type Rules = Awaited<ReturnType<typeof loadImportRules>>;
type Kind =
  | "price"
  | "variant_options"
  | "variable_to_simple"
  | "simple_to_variable"
  | "image"
  | "missing"
  | "out_of_stock";
type State = {
  id: string;
  status: string;
  publishedOnPublication: boolean;
  variants: {
    nodes: {
      id: string;
      sku: string;
      price: string;
      compareAtPrice: string | null;
      selectedOptions: { name: string; value: string }[];
      inventoryItem: {
        inventoryLevel: {
          quantities: { name: string; quantity: number }[];
        } | null;
      };
    }[];
    pageInfo: { hasNextPage: boolean };
  };
  media: { nodes: { id: string }[]; pageInfo: { hasNextPage: boolean } };
};
type TestCase = {
  kind: Kind;
  before: SourceProduct;
  after: SourceProduct | null;
  original: State;
};
type Manifest = {
  shop: string;
  locationId: string;
  publicationId: string;
  rulesHash: string;
  baselineHash: string;
  changedHash: string;
  originalRows: number;
  originalEligible: number;
  changedEligible: number;
  cases: TestCase[];
};
const READ = `#graphql
  query ChangedFeedProduct($identifier: ProductIdentifierInput!, $locationId: ID!, $publicationId: ID!) {
    productByIdentifier(identifier: $identifier) {
      id status publishedOnPublication(publicationId: $publicationId)
      variants(first: 25) {
        nodes {
          id sku price compareAtPrice selectedOptions { name value }
          inventoryItem {
            inventoryLevel(locationId: $locationId) {
              quantities(names: ["available"]) { name quantity }
            }
          }
        }
        pageInfo { hasNextPage }
      }
      media(first: 10) { nodes { id } pageInfo { hasNextPage } }
    }
  }
`;
function hashRules(rules: Rules) {
  return createHash("sha256")
    .update(JSON.stringify(rules.snapshot))
    .digest("hex");
}
function key(product: SourceProduct) {
  return `${product.storeId}:${product.productId}`;
}
function csvCell(value: string) {
  return `"${value.replace(/"/g, '""')}"`;
}
function available(variant: State["variants"]["nodes"][number]) {
  return variant.inventoryItem.inventoryLevel?.quantities.find(
    (item) => item.name === "available",
  )?.quantity;
}
function sameSet(a: string[], b: string[]) {
  return (
    a.length === b.length &&
    [...a].sort().every((value, index) => value === [...b].sort()[index])
  );
}
function expected(product: SourceProduct, rules: Rules) {
  const decision = rules.evaluate(product);
  if (!decision.eligible)
    throw new Error(
      `Test product ${key(product)} is excluded by the current settings.`,
    );
  return decision;
}
async function reader(shop: string) {
  const admin = createImportAdmin(shop);
  const api = createRequester(admin);
  return {
    publication: () =>
      createProductPublisher(admin).onlineStorePublication(() => {}),
    restoredImageIds: async (urls: string[]) => {
      const ids: string[] = [];
      for (const url of [...new Set(urls)]) {
        const name = sourceImageFilename(url);
        const stem = name.slice(0, -extname(name).length);
        const result = await api.request<{
          files: {
            nodes: {
              __typename: string;
              id: string;
              fileStatus: string;
              status?: string;
              image?: { url: string } | null;
            }[];
          };
        }>(
          `#graphql
            query RestoredSourceImage($query: String!) {
              files(first: 2, query: $query) {
                nodes {
                  __typename id fileStatus
                  ... on MediaImage { status image { url } }
                }
              }
            }
          `,
          { query: `filename:${stem}*` },
          20,
        );
        const file = result.files.nodes[0];
        if (
          result.files.nodes.length !== 1 ||
          file.__typename !== "MediaImage" ||
          file.fileStatus !== "READY" ||
          file.status !== "READY" ||
          !file.image?.url
        )
          throw new Error(
            `Cannot verify a unique ready Shopify file for restored source image: ${url}`,
          );
        ids.push(file.id);
      }
      return ids;
    },
    read: async (
      product: SourceProduct,
      locationId: string,
      publicationId: string,
    ) => {
      const data = await api.request<{ productByIdentifier: State | null }>(
        READ,
        {
          identifier: {
            customId: {
              namespace: "$app",
              key: "source_product_id",
              value: key(product),
            },
          },
          locationId,
          publicationId,
        },
        200,
      );
      const state = data.productByIdentifier;
      if (!state)
        throw new Error(
          `Product ${key(product)} was not found. Finish the baseline import first.`,
        );
      if (
        state.variants.pageInfo.hasNextPage ||
        state.media.pageInfo.hasNextPage
      )
        throw new Error(
          `Test product ${key(product)} exceeds the diagnostic's variant/image limit.`,
        );
      return state;
    },
  };
}
export function selectChanges(products: SourceProduct[], rules: Rules) {
  const used = new Set<string>();
  const selected: {
    kind: Kind;
    before: SourceProduct;
    after: SourceProduct | null;
  }[] = [];
  const choose = (
    kind: Kind,
    predicate: (product: SourceProduct) => boolean,
  ) => {
    const product = products.find(
      (item) =>
        !used.has(key(item)) && item.variants.length <= 20 && predicate(item),
    );
    if (!product) throw new Error(`No suitable eligible product for ${kind}.`);
    used.add(key(product));
    return structuredClone(product);
  };
  const price = choose("price", (product) => {
    const amount = BigInt(product.currentPrice.replace(".", "")) + 10000n;
    return rules.evaluate({
      ...product,
      currentPrice: `${amount / 100n}.${String(amount % 100n).padStart(2, "0")}`,
    }).eligible;
  });
  const amount = BigInt(price.currentPrice.replace(".", "")) + 10000n;
  const newPrice = `${amount / 100n}.${String(amount % 100n).padStart(2, "0")}`;
  selected.push({
    kind: "price",
    before: price,
    after: {
      ...price,
      currentPrice: newPrice,
      raw: { ...price.raw, current_price: newPrice },
    },
  });
  const options = choose(
    "variant_options",
    (product) => product.hasVariants && product.variants.length >= 2,
  );
  const updatedVariants = [
    ...options.variants.slice(0, -1),
    { name: options.variants[0].name, value: `SYNC-QA-${options.productId}` },
  ];
  selected.push({
    kind: "variant_options",
    before: options,
    after: {
      ...options,
      variants: updatedVariants,
      raw: { ...options.raw, variants: JSON.stringify(updatedVariants) },
    },
  });
  const variable = choose(
    "variable_to_simple",
    (product) => product.hasVariants,
  );
  selected.push({
    kind: "variable_to_simple",
    before: variable,
    after: {
      ...variable,
      hasVariants: false,
      variants: [],
      raw: { ...variable.raw, has_variants: "0", variants: "[]" },
    },
  });
  const simple = choose(
    "simple_to_variable",
    (product) => !product.hasVariants,
  );
  const sizes = [
    { name: "Size", value: "SYNC-QA-1" },
    { name: "Size", value: "SYNC-QA-2" },
  ];
  selected.push({
    kind: "simple_to_variable",
    before: simple,
    after: {
      ...simple,
      hasVariants: true,
      variants: sizes,
      raw: {
        ...simple.raw,
        has_variants: "1",
        variants: JSON.stringify(sizes),
      },
    },
  });
  const image = choose(
    "image",
    (product) => product.imageUrls[0] !== price.imageUrls[0],
  );
  selected.push({
    kind: "image",
    before: image,
    after: {
      ...image,
      imageUrls: [price.imageUrls[0]],
      raw: { ...image.raw, image_url: price.imageUrls[0], product_images: "" },
    },
  });
  const missing = choose("missing", () => true);
  selected.push({ kind: "missing", before: missing, after: null });
  const stock = choose("out_of_stock", () => true);
  selected.push({
    kind: "out_of_stock",
    before: stock,
    after: {
      ...stock,
      inStock: false,
      raw: { ...stock.raw, stock_status: "outofstock" },
    },
  });
  for (const item of selected) if (item.after) normalizeVariants(item.after);
  return selected;
}

export async function prepareTest(shop: string, filePath: string) {
  const sourceHash = await hashImportFile(filePath);
  const rules = await loadImportRules(shop);
  const account = await db.accountSettings.findUniqueOrThrow({
    where: { shop },
  });
  if (!account.inventoryLocationId)
    throw new Error("Finish a baseline inventory import first.");
  const products: SourceProduct[] = [];
  let rows = 0;
  for await (const row of readSourceCsv(filePath)) {
    rows++;
    if (!row.ok) throw new Error(`Invalid row ${row.line}: ${row.error}`);
    if (rules.evaluate(row.product).eligible) products.push(row.product);
  }
  const chosen = selectChanges(products, rules);
  const api = await reader(shop);
  const publicationId = await api.publication();
  const cases: TestCase[] = [];
  for (const item of chosen) {
    const original = await api.read(
      item.before,
      account.inventoryLocationId,
      publicationId,
    );
    const decision = expected(item.before, rules);
    const skus = normalizeVariants(item.before).variants.map(
      (variant) => variant.sku,
    );
    if (
      original.status !== "ACTIVE" ||
      !original.publishedOnPublication ||
      !sameSet(
        original.variants.nodes.map((variant) => variant.sku),
        skus,
      ) ||
      original.variants.nodes.some(
        (variant) =>
          Number(variant.price) !== Number(decision.sellingPrice) ||
          available(variant) !== 1000,
      ) ||
      !original.media.nodes.length
    )
      throw new Error(
        `Baseline product ${key(item.before)} differs from the CSV/settings. Complete an unchanged baseline import before preparing this test.`,
      );
    cases.push({ ...item, original });
  }
  const folder = await mkdir(resolve("var/imports"), { recursive: true }).then(
    async () => {
      const { mkdtemp } = await import("node:fs/promises");
      return mkdtemp(resolve("var/imports/change-test-"));
    },
  );
  const baseline = join(folder, "baseline.csv");
  await copyFile(filePath, baseline);
  if ((await hashImportFile(baseline)) !== sourceHash)
    throw new Error(
      "The source CSV changed during preparation. Do not import this test folder; prepare again from an unchanged CSV.",
    );
  // The full feed is retained. Only one selected row is omitted.
  const changes = new Map(cases.map((item) => [key(item.before), item.after]));
  const lines: string[] = [];
  let header: string[] | undefined;
  let changedEligible = 0;
  for await (const row of readSourceCsv(baseline)) {
    if (!row.ok) throw new Error(`Invalid baseline row ${row.line}.`);
    if (!header) {
      header = Object.keys(row.product.raw);
      lines.push(header.map(csvCell).join(","));
    }
    const changed = changes.has(key(row.product))
      ? changes.get(key(row.product))
      : row.product;
    if (!changed) continue;
    const raw = changed.raw;
    lines.push(header.map((column) => csvCell(raw[column] ?? "")).join(","));
    if (rules.evaluate(changed).eligible) changedEligible++;
  }
  const changedPath = join(folder, "changed.csv");
  await writeFile(changedPath, lines.join("\n") + "\n");
  // Reparse the generated CSV before presenting it as importable.
  let reparsed = 0;
  for await (const row of readSourceCsv(changedPath)) {
    if (!row.ok) throw new Error(`Generated CSV row ${row.line}: ${row.error}`);
    normalizeVariants(row.product);
    reparsed++;
  }
  if (reparsed !== rows - 1 || changedEligible !== products.length - 2)
    throw new Error("Unexpected test feed counts. Do not import this fixture.");
  const manifest: Manifest = {
    shop,
    locationId: account.inventoryLocationId,
    publicationId,
    rulesHash: hashRules(rules),
    baselineHash: await hashImportFile(baseline),
    changedHash: await hashImportFile(changedPath),
    originalRows: rows,
    originalEligible: products.length,
    changedEligible,
    cases,
  };
  await writeFile(join(folder, "test.json"), JSON.stringify(manifest, null, 2));
  console.table(
    cases.map((item) => ({
      test: item.kind,
      sourceId: key(item.before),
      productId: item.original.id,
    })),
  );
  console.log(`Test folder: ${folder}`);
  console.log(`Changed CSV: ${changedPath}`);
  console.log(`Restore CSV: ${baseline}`);
  console.log(
    `Eligible products: ${products.length} baseline; ${changedEligible} changed feed.`,
  );
  console.log(
    "Preparation only: no Shopify products or account settings were changed. Keep these settings/mappings unchanged until verification and restoration finish.",
  );
  return folder;
}

export async function verifyTest(
  shop: string,
  folder: string,
  restore = false,
) {
  const manifest = JSON.parse(
    await readFile(join(folder, "test.json"), "utf8"),
  ) as Manifest;
  if (manifest.shop !== shop)
    throw new Error("This test belongs to another shop.");
  const rules = await loadImportRules(shop);
  if (hashRules(rules) !== manifest.rulesHash)
    throw new Error(
      "Settings or mappings changed after test preparation. Restore the original settings before continuing.",
    );
  const csv = join(folder, restore ? "baseline.csv" : "changed.csv");
  const hash = restore ? manifest.baselineHash : manifest.changedHash;
  if ((await hashImportFile(csv)) !== hash)
    throw new Error("The test CSV changed. Do not use this test manifest.");
  const job = await db.importJob.findFirst({
    where: { shop, fileHash: hash, status: "completed" },
    orderBy: { createdAt: "desc" },
  });
  if (
    !job ||
    job.eligible !==
      (restore ? manifest.originalEligible : manifest.changedEligible)
  )
    throw new Error("Complete the matching CSV import before verifying.");
  const api = await reader(shop);
  const failures: string[] = [];
  const result: { test: Kind; sourceId: string; result: string }[] = [];
  for (const item of manifest.cases) {
    const state = await api.read(
      item.before,
      manifest.locationId,
      manifest.publicationId,
    );
    const errors: string[] = [];
    if (state.id !== item.original.id) errors.push("product ID changed");
    const product = restore ? item.before : item.after;
    const eligible = product && rules.evaluate(product).eligible;
    if (!eligible) {
      if (
        !sameSet(
          state.variants.nodes.map((variant) => variant.sku),
          item.original.variants.nodes.map((variant) => variant.sku),
        )
      )
        errors.push("skipped product variants changed");
      if (state.variants.nodes.some((variant) => available(variant) !== 0))
        errors.push(
          "missing/out-of-stock product is not zero at the import location",
        );
    } else {
      const decision = expected(product!, rules);
      const variants = normalizeVariants(product!).variants;
      if (
        !sameSet(
          state.variants.nodes.map((variant) => variant.sku),
          variants.map((variant) => variant.sku),
        )
      )
        errors.push("variant SKU set differs");
      for (const variant of state.variants.nodes) {
        if (Number(variant.price) !== Number(decision.sellingPrice))
          errors.push("selling price differs");
        if (
          (variant.compareAtPrice === null
            ? null
            : Number(variant.compareAtPrice)) !==
          (decision.compareAtPrice === null
            ? null
            : Number(decision.compareAtPrice))
        )
          errors.push("compare-at price differs");
        if (available(variant) !== 1000)
          errors.push("eligible variant is not stocked");
        const source = variants.find((value) => value.sku === variant.sku);
        const expectedOptions = source?.optionValues.length
          ? source.optionValues
          : [{ name: "Title", value: "Default Title" }];
        if (
          !sameSet(
            variant.selectedOptions.map(
              (option) => `${option.name}=${option.value}`,
            ),
            expectedOptions.map((option) => `${option.name}=${option.value}`),
          )
        )
          errors.push("variant options differ");
        // Retained option combinations keep their Shopify IDs. Removed and later
        // recreated variants may receive new IDs during restoration.
        if (!restore) {
          const old = item.original.variants.nodes.find(
            (value) => value.sku === variant.sku,
          );
          if (old && old.id !== variant.id)
            errors.push("retained variant ID changed");
        }
      }
      if (state.status !== "ACTIVE" || !state.publishedOnPublication)
        errors.push("not active/published");
    }
    const mediaIds =
      !restore && item.kind === "image"
        ? [
            manifest.cases.find((value) => value.kind === "price")!.original
              .media.nodes[0].id,
          ]
        : item.original.media.nodes.map((media) => media.id);
    const attachedIds = state.media.nodes.map((media) => media.id);
    if (!sameSet(attachedIds, mediaIds)) {
      if (restore && item.kind === "image") {
        // A removed file can be recreated when its original URL returns. Verify
        // source identity rather than requiring a deleted media ID to return.
        const restoredIds = await api.restoredImageIds(item.before.imageUrls);
        if (!sameSet(attachedIds, restoredIds))
          errors.push("attached images do not match the restored source URLs");
        else
          console.log(
            `Image ${key(item.before)}: original source image verified; Shopify media ID changed.`,
          );
      } else {
        errors.push("image associations differ");
      }
    }
    const message = errors.length ? [...new Set(errors)].join("; ") : "PASS";
    result.push({
      test: item.kind,
      sourceId: key(item.before),
      result: message,
    });
    if (errors.length) failures.push(`${item.kind}: ${message}`);
  }
  console.table(result);
  if (failures.length) throw new Error(failures.join("\n"));
  console.log(
    restore
      ? "PASS: baseline restored for all seven test products."
      : "PASS: all seven changed-feed cases verified against Shopify.",
  );
}

async function main() {
  const [command, shop, path, ...extra] = process.argv.slice(2);
  if (
    !shop ||
    !path ||
    extra.length ||
    !["prepare", "verify", "verify-restored"].includes(command)
  )
    throw new Error(
      "Usage: check-import-updates.ts prepare <shop> <original-csv> | verify <shop> <test-folder> | verify-restored <shop> <test-folder>",
    );
  checkImportShop(shop);
  if (
    shop !== "jahan-dev-store.myshopify.com" ||
    process.env.NODE_ENV === "production"
  )
    throw new Error(
      "This diagnostic is restricted to the local jahan-dev-store development setup.",
    );
  if (await db.importJob.findUnique({ where: { activeShop: shop } }))
    throw new Error(
      "Finish the active import before preparing or verifying this test.",
    );
  if (command === "prepare") await prepareTest(shop, resolve(path));
  else await verifyTest(shop, resolve(path), command === "verify-restored");
}
if (
  process.argv[1] &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  void main()
    .catch((error: unknown) => {
      console.error(
        error instanceof Error
          ? error.message
          : "Changed-feed diagnostic failed.",
      );
      process.exitCode = 1;
    })
    .finally(() => db.$disconnect());
}
