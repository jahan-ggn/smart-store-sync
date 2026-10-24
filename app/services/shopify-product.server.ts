import { setTimeout as delay } from "node:timers/promises";
import type { unauthenticated } from "../shopify.server";
import type { SourceProduct } from "./source-csv.server";
import type { ImportDecision } from "./import-rules.server";
import type { NormalizedVariants } from "./variants.server";

type Admin = Awaited<ReturnType<typeof unauthenticated.admin>>["admin"];
export type PreparedProduct = {
  product: SourceProduct;
  variants: NormalizedVariants;
  decision: Extract<ImportDecision, { eligible: true }>;
};
type ApiError = { message: string; extensions?: { code?: string } };
type Cost = {
  requestedQueryCost?: number;
  actualQueryCost?: number | null;
  throttleStatus?: {
    maximumAvailable: number;
    currentlyAvailable: number;
    restoreRate: number;
  };
};
type Envelope<T> = {
  data?: T;
  errors?: ApiError[];
  extensions?: { cost?: Cost };
};
type ThrownApiError = {
  body?: {
    errors?: { graphQLErrors?: ApiError[] };
    extensions?: { cost?: Cost };
  };
  response?: {
    code?: number;
    retryAfter?: number;
    body?: { extensions?: { cost?: Cost } };
  };
};
type ExistingVariant = {
  id: string;
  sku: string | null;
  price: string;
  compareAtPrice: string | null;
  selectedOptions: { name: string; value: string }[];
};
type VariantPage = {
  nodes: ExistingVariant[];
  pageInfo: { hasNextPage: boolean; endCursor: string | null };
};
type ExistingProduct = { id: string; title: string; variants: VariantPage };

const VARIANT_FIELDS = `
  nodes { id sku price compareAtPrice selectedOptions { name value } }
  pageInfo { hasNextPage endCursor }
`;
const MORE_VARIANTS = `#graphql
  query MoreSourceVariants($id: ID!, $after: String!) {
    product(id: $id) {
      id
      variants(first: 100, after: $after) { ${VARIANT_FIELDS} }
    }
  }
`;
const UPSERT = `#graphql
  mutation SyncSourceProduct($identifier: ProductSetIdentifiers!, $input: ProductSetInput!) {
    productSet(identifier: $identifier, input: $input, synchronous: true) {
      product {
        id
        sourceId: metafield(namespace: "$app", key: "source_product_id") { jsonValue }
      }
      userErrors { field message code }
    }
  }
`;

function createRequester(admin: Admin) {
  let available: number | undefined;
  let maximum = 0;
  let restoreRate = 0;
  let updatedAt = performance.now();
  let reservations = Promise.resolve();
  let requests = 0;
  const knownCosts = new Map<string, number>();
  function refill() {
    const now = performance.now();
    if (available !== undefined)
      available = Math.min(
        maximum,
        available + (Math.max(0, now - updatedAt) * restoreRate) / 1000,
      );
    updatedAt = now;
  }
  async function reserve(cost: number) {
    let reserved: number | undefined;
    const task = reservations.then(async () => {
      if (available === undefined) return;
      if (cost > maximum)
        throw new Error(
          "This Shopify query exceeds the available API bucket size.",
        );
      refill();

      while (available! < cost) {
        if (restoreRate <= 0) {
          throw new Error(
            "Shopify's API budget is not restoring. Try again later.",
          );
        }

        await delay(
          Math.min(
            10_000,
            Math.max(1, Math.ceil(((cost - available!) * 1000) / restoreRate)),
          ),
        );

        refill();
      }
      available! -= cost;
      reserved = cost;
    });
    reservations = task.then(
      () => undefined,
      () => undefined,
    );
    await task;
    return reserved;
  }
  function observe(cost: Cost | undefined, reserved: number | undefined) {
    const throttle = cost?.throttleStatus;
    if (
      !throttle ||
      ![
        throttle.maximumAvailable,
        throttle.currentlyAvailable,
        throttle.restoreRate,
      ].every(Number.isFinite)
    )
      return;
    refill();
    maximum = Math.max(0, throttle.maximumAvailable);
    restoreRate = Math.max(0, throttle.restoreRate);
    const reported = Math.max(0, throttle.currentlyAvailable);
    if (available === undefined) available = Math.min(maximum, reported);
    else {
      if (reserved !== undefined && typeof cost?.actualQueryCost === "number")
        available = Math.max(0, available + reserved - cost.actualQueryCost);
      available = Math.min(maximum, available, reported);
    }
  }
  async function request<T>(
    query: string,
    variables: Record<string, unknown>,
    estimate = 20,
    beforeRequest: () => void = () => {},
  ): Promise<T> {
    for (let attempt = 0; attempt < 4; attempt++) {
      const reserved = await reserve(knownCosts.get(query) ?? estimate);
      let throttled = false;
      let retryAfter = 0;
      beforeRequest();
      requests++;
      try {
        const response = await admin.graphql(query, { variables, tries: 1 });
        const body = (await response.json()) as Envelope<T>;
        const cost = body.extensions?.cost;
        observe(cost, reserved);
        if (typeof cost?.requestedQueryCost === "number")
          knownCosts.set(query, cost.requestedQueryCost);
        const errors = body.errors ?? [];
        throttled =
          response.status === 429 ||
          (errors.length > 0 &&
            errors.every((error) => error.extensions?.code === "THROTTLED"));
        if (!throttled) {
          if (!response.ok || errors.length || !body.data)
            throw new Error(
              errors.map((error) => error.message).join("; ") ||
                `Shopify request failed (${response.status}).`,
            );
          return body.data;
        }
        retryAfter = Number(response.headers.get("retry-after")) || 0;
      } catch (error) {
        const failure = error as ThrownApiError | null;
        const errors = failure?.body?.errors?.graphQLErrors ?? [];
        throttled =
          failure?.response?.code === 429 ||
          (errors.length > 0 &&
            errors.every((value) => value.extensions?.code === "THROTTLED"));
        if (!throttled) throw error;
        const cost =
          failure?.body?.extensions?.cost ??
          failure?.response?.body?.extensions?.cost;
        observe(cost, reserved);
        if (typeof cost?.requestedQueryCost === "number")
          knownCosts.set(query, cost.requestedQueryCost);
        retryAfter = failure?.response?.retryAfter ?? 0;
      }
      if (attempt === 3)
        throw new Error(
          "Shopify is still throttling this import. Resume it later.",
        );
      await delay(Math.max(1000 * 2 ** attempt, retryAfter * 1000));
    }
    throw new Error("Shopify request did not complete.");
  }
  return {
    request,
    get requestCount() {
      return requests;
    },
  };
}

function signature(options: { name: string; value: string }[]) {
  return JSON.stringify(
    options
      .map((option) => [
        option.name.normalize("NFC").trim().replace(/\s+/g, " ").toLowerCase(),
        option.value.normalize("NFC").trim(),
      ])
      .sort(([a], [b]) => a.localeCompare(b)),
  );
}
function moneyValue(value: string | null) {
  if (value === null) return null;
  if (!/^\d+(?:\.\d{1,2})?$/.test(value)) return value;
  const [whole, fraction = ""] = value.split(".");
  return BigInt(whole) * 100n + BigInt(fraction.padEnd(2, "0"));
}
function sourceIdentifier(payload: PreparedProduct) {
  return {
    customId: {
      namespace: "$app",
      key: "source_product_id",
      value: `${payload.product.storeId}:${payload.product.productId}`,
    },
  };
}

export function createProductImporter(admin: Admin, concurrency = 3) {
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 3)
    throw new Error("Importer concurrency must be between 1 and 3.");
  const api = createRequester(admin);
  async function lookupGroup(products: PreparedProduct[]) {
    const variables: Record<string, unknown> = {};
    const declarations = products
      .map((_, i) => `$p${i}: ProductIdentifierInput!`)
      .join(", ");
    const fields = products
      .map((product, i) => {
        variables[`p${i}`] = sourceIdentifier(product);
        return `p${i}: productByIdentifier(identifier: $p${i}) { id title variants(first: 10) { ${VARIANT_FIELDS} } }`;
      })
      .join("\n");
    const result = await api.request<Record<string, ExistingProduct | null>>(
      `#graphql\nquery LookupSourceGroup(${declarations}) { ${fields} }`,
      variables,
      products.length * 32 + 1,
    );
    const existing: {
      id: string;
      title: string;
      variants: ExistingVariant[];
    }[] = [];
    for (let i = 0; i < products.length; i++) {
      const product = result[`p${i}`];
      if (product === undefined)
        throw new Error("Shopify omitted a product lookup result.");
      if (!product) {
        existing.push({ id: "", title: "", variants: [] });
        continue;
      }
      const variants = [...product.variants.nodes];
      let page = product.variants;
      while (page.pageInfo.hasNextPage) {
        const after = page.pageInfo.endCursor;
        if (!after) throw new Error("Missing Shopify variant cursor.");
        const more: { product: ExistingProduct | null } = await api.request(
          MORE_VARIANTS,
          { id: product.id, after },
          250,
        );
        if (!more.product || more.product.id !== product.id)
          throw new Error("The product disappeared while loading variants.");
        page = more.product.variants;
        variants.push(...page.nodes);
        if (page.pageInfo.hasNextPage && page.pageInfo.endCursor === after)
          throw new Error("Shopify repeated a variant cursor.");
      }
      existing.push({ id: product.id, title: product.title, variants });
    }
    return existing;
  }
  async function syncGroup(
    products: PreparedProduct[],
    beforeWrite: () => void = () => {},
  ) {
    if (!products.length) return [];
    if (products.length > 25)
      throw new Error("Sync at most 25 products per group.");
    const keys = products.map(
      (product) => sourceIdentifier(product).customId.value,
    );
    if (new Set(keys).size !== keys.length)
      throw new Error("The import group contains duplicate source products.");
    const existing = await lookupGroup(products);
    const results: { sourceId: string; productId: string; title: string }[] =
      new Array(products.length);
    let next = 0;
    let failed = false;
    let firstError: unknown;
    async function worker() {
      while (!failed && next < products.length) {
        const i = next++;
        try {
          beforeWrite();
          const { product, variants, decision } = products[i];
          if (!decision.eligible || !variants.variants.length)
            throw new Error("The prepared product is not eligible.");
          const ids = new Map(
            existing[i].variants.map((variant) => [
              signature(variant.selectedOptions),
              variant.id,
            ]),
          );
          const options = variants.options.length
            ? variants.options
            : [{ name: "Title", values: ["Default Title"] }];
          const expected = variants.variants.map((variant) => ({
            variant,
            selected: variant.optionValues.length
              ? variant.optionValues
              : [{ name: "Title", value: "Default Title" }],
          }));
          const existingByOptions = new Map(
            existing[i].variants.map((variant) => [
              signature(variant.selectedOptions),
              variant,
            ]),
          );
          const unchanged =
            existing[i].id &&
            existing[i].title === product.name &&
            existing[i].variants.length === expected.length &&
            expected.every(({ variant, selected }, position) => {
              const saved = existingByOptions.get(signature(selected));
              return (
                saved &&
                saved.id === existing[i].variants[position].id &&
                saved.sku === variant.sku &&
                moneyValue(saved.price) === moneyValue(decision.sellingPrice) &&
                moneyValue(saved.compareAtPrice) ===
                  moneyValue(decision.compareAtPrice) &&
                selected.every((option) =>
                  saved.selectedOptions.some(
                    (old) =>
                      old.name === option.name && old.value === option.value,
                  ),
                )
              );
            });
          if (unchanged) {
            results[i] = {
              sourceId: keys[i],
              productId: existing[i].id,
              title: product.name,
            };
            continue;
          }
          const input = {
            title: product.name,
            ...(existing[i].id ? {} : { status: "DRAFT" }),
            productOptions: options.map((option, position) => ({
              name: option.name,
              position: position + 1,
              values: option.values.map((name) => ({ name })),
            })),
            variants: variants.variants.map((variant) => {
              const selected = variant.optionValues.length
                ? variant.optionValues
                : [{ name: "Title", value: "Default Title" }];
              const id = ids.get(signature(selected));
              return {
                ...(id ? { id } : {}),
                sku: variant.sku,
                price: decision.sellingPrice,
                compareAtPrice: decision.compareAtPrice,
                optionValues: selected.map((option) => ({
                  optionName: option.name,
                  name: option.value,
                })),
              };
            }),
          };
          const data = await api.request<{
            productSet: {
              product: {
                id: string;
                sourceId: { jsonValue: unknown } | null;
              } | null;
              userErrors: {
                field: string[] | null;
                message: string;
                code: string;
              }[];
            };
          }>(
            UPSERT,
            { identifier: sourceIdentifier(products[i]), input },
            20,
            beforeWrite,
          );
          const saved = data.productSet;
          if (saved.userErrors.length || !saved.product)
            throw new Error(
              saved.userErrors
                .map(
                  (error) =>
                    `${error.field?.join(".") || "product"}: ${error.message}`,
                )
                .join("; ") || "Shopify did not return a product.",
            );
          if (
            saved.product.sourceId?.jsonValue !== keys[i] ||
            (existing[i].id && saved.product.id !== existing[i].id)
          )
            throw new Error("Shopify returned an unexpected product identity.");
          results[i] = {
            sourceId: keys[i],
            productId: saved.product.id,
            title: product.name,
          };
        } catch (error) {
          failed = true;
          firstError ??= error;
        }
      }
    }
    // Drain every in-flight write before the caller may retry the group.
    await Promise.all(
      Array.from({ length: Math.min(concurrency, products.length) }, () =>
        worker(),
      ),
    );
    if (failed) throw firstError ?? new Error("Product group failed.");
    return results;
  }
  return {
    syncGroup,
    get requestCount() {
      return api.requestCount;
    },
  };
}

export async function syncPreparedProduct(
  admin: Admin,
  product: PreparedProduct,
) {
  return (await createProductImporter(admin, 1).syncGroup([product]))[0];
}

export async function ensureSourceProductDefinition(admin: Admin) {
  const api = createRequester(admin);
  const lookup = async () => {
    const result = await api.request<{
      metafieldDefinitions: { nodes: { id: string; type: { name: string } }[] };
    }>(
      `#graphql
      query SourceProductDefinition {
        metafieldDefinitions(first: 1, ownerType: PRODUCT, namespace: "$app", key: "source_product_id") {
          nodes { id type { name } }
        }
      }
    `,
      {},
    );
    return result.metafieldDefinitions.nodes[0];
  };
  const check = (definition: { id: string; type: { name: string } }) => {
    if (definition.type.name !== "id")
      throw new Error(
        "The source_product_id metafield exists with the wrong type. Expected id.",
      );
    return definition.id;
  };
  const existing = await lookup();
  if (existing) return check(existing);
  const result = await api.request<{
    metafieldDefinitionCreate: {
      createdDefinition: { id: string; type: { name: string } } | null;
      userErrors: { message: string }[];
    };
  }>(
    `#graphql
    mutation CreateSourceProductDefinition {
      metafieldDefinitionCreate(definition: {
        name: "Smart Store Sync source ID", namespace: "$app", key: "source_product_id",
        type: "id", ownerType: PRODUCT, access: { admin: MERCHANT_READ }
      }) {
        createdDefinition { id type { name } }
        userErrors { message }
      }
    }
  `,
    {},
  );
  const created = result.metafieldDefinitionCreate;
  if (created.userErrors.length || !created.createdDefinition) {
    const concurrent = await lookup();
    if (concurrent) return check(concurrent);
    throw new Error(
      created.userErrors.map((error) => error.message).join("; ") ||
        "Unable to create the source identifier definition.",
    );
  }
  return check(created.createdDefinition);
}
