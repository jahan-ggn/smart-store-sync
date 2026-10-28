import { createHash } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { extname } from "node:path";
import { createRequester } from "./shopify-product.server";
import type { SourceProduct } from "./source-csv.server";

type Admin = Parameters<typeof createRequester>[0];
type File = {
  __typename: string;
  id: string;
  fileStatus: string;
  status?: string;
  fileErrors: { code: string; message: string }[];
  image?: { url: string } | null;
};
type MediaPage = {
  nodes: { id: string }[];
  pageInfo: { hasNextPage: boolean; endCursor: string | null };
};
type Product = { id: string; media: MediaPage };
type Errors = { field: string[] | null; message: string }[];
const FILE_FIELDS = `
  __typename id fileStatus fileErrors { code message }
  ... on MediaImage { status image { url } }
`;
const CREATE = `#graphql
  mutation CreateSourceImages($files: [FileCreateInput!]!) {
    fileCreate(files: $files) {
      files { __typename id fileStatus fileErrors { code message } }
      userErrors { field message }
    }
  }
`;
const CHECK = `#graphql
  query SourceImagesReady($ids: [ID!]!) {
    nodes(ids: $ids) { ... on MediaImage { ${FILE_FIELDS} } }
  }
`;
const MORE = `#graphql
  query SourceProductMoreMedia($id: ID!, $after: String!) {
    product(id: $id) {
      id
      media(first: 100, after: $after) {
        nodes { id }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
`;
const ATTACH = `#graphql
  mutation SetSourceProductImages($identifier: ProductSetIdentifiers!, $input: ProductSetInput!, $count: Int!) {
    productSet(identifier: $identifier, input: $input, synchronous: true) {
      product { id media(first: $count) { nodes { id } } }
      userErrors { field message }
    }
  }
`;
const ORDER = `#graphql
  mutation OrderSourceProductImages($id: ID!, $moves: [MoveInput!]!) {
    productReorderMedia(id: $id, moves: $moves) {
      job { id }
      mediaUserErrors { field message }
    }
  }
`;
const JOB = `#graphql
  query SourceImageOrderJob($id: ID!) { job(id: $id) { id done } }
`;
function assertNoErrors(errors: Errors) {
  if (errors.length)
    throw new Error(errors.map((error) => error.message).join("; "));
}
export function sourceImageFilename(source: string) {
  const url = new URL(source);
  if (
    !["https:", "http:"].includes(url.protocol) ||
    url.username ||
    url.password
  )
    throw new Error("Invalid source image URL.");
  const supplied = extname(url.pathname).toLowerCase();
  const extension = [
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
    ".avif",
  ].includes(supplied)
    ? supplied
    : ".jpg";
  return `cartpe-sync-${createHash("sha256").update(source).digest("hex")}${extension}`;
}
function sameIds(a: string[], b: string[]) {
  return a.length === b.length && a.every((id, index) => id === b[index]);
}

export function createImageImporter(admin: Admin) {
  const api = createRequester(admin);
  const knownFiles = new Map<string, File>();
  const downloadRetries = new Map<string, number>();
  const request: typeof api.request = (
    query,
    variables,
    estimate = 20,
    guard = () => {},
  ) => api.request(query, variables, estimate, guard);
  const lookup = async (products: SourceProduct[], guard: () => void) => {
    const variables: Record<string, unknown> = {};
    const declarations = products
      .map((_, i) => `$p${i}: ProductIdentifierInput!`)
      .join(", ");
    const fields = products
      .map((product, i) => {
        variables[`p${i}`] = {
          customId: {
            namespace: "$app",
            key: "source_product_id",
            value: `${product.storeId}:${product.productId}`,
          },
        };
        return `p${i}: productByIdentifier(identifier: $p${i}) { id media(first: 10) { nodes { id } pageInfo { hasNextPage endCursor } } }`;
      })
      .join("\n");
    const data = await request<Record<string, Product | null>>(
      `#graphql\nquery SourceImageProducts(${declarations}) { ${fields} }`,
      variables,
      400,
      guard,
    );
    const found: { id: string; mediaIds: string[] }[] = [];
    for (let i = 0; i < products.length; i++) {
      const product = data[`p${i}`];
      if (!product)
        throw new Error(
          `Imported product ${products[i].storeId}:${products[i].productId} is missing. Run a fresh product import.`,
        );
      const ids = product.media.nodes.map((media) => media.id);
      let page = product.media;
      const seen = new Set<string>();
      while (page.pageInfo.hasNextPage) {
        const after = page.pageInfo.endCursor;
        if (!after || seen.has(after))
          throw new Error("Invalid product media cursor.");
        seen.add(after);
        const more = await request<{ product: Product | null }>(
          MORE,
          { id: product.id, after },
          150,
          guard,
        );
        if (!more.product || more.product.id !== product.id)
          throw new Error("Product disappeared during image sync.");
        page = more.product.media;
        ids.push(...page.nodes.map((media) => media.id));
      }
      found.push({ id: product.id, mediaIds: ids });
    }
    return found;
  };
  const prepareGroup = async (
    products: SourceProduct[],
    guard: () => void = () => {},
  ) => {
    if (!products.length) return;
    if (products.length > 250)
      throw new Error("Prepare at most 250 image products per group.");
    const entries = new Map<string, { url: string; title: string }>();
    const names = products.map((product) => {
      const urls = [...new Set(product.imageUrls)];
      if (!urls.length || urls.length > 250)
        throw new Error("A product must have between 1 and 250 image URLs.");
      return urls.map((url) => {
        const name = sourceImageFilename(url);
        entries.set(name, { url, title: product.name });
        return name;
      });
    });
    const files = new Map<string, File>();
    // Resolve filenames together rather than one API call per image.
    const allNames = [...entries.keys()];
    for (const name of allNames)
      if (knownFiles.has(name)) files.set(name, knownFiles.get(name)!);
    const uncached = allNames.filter((name) => !files.has(name));
    for (let offset = 0; offset < uncached.length; offset += 25) {
      const batch = uncached.slice(offset, offset + 25);
      const variables: Record<string, unknown> = {};
      const declarations = batch
        .map((_, index) => `$q${index}: String!`)
        .join(", ");
      const fields = batch
        .map((name, index) => {
          // Search by the full URL hash, allowing Shopify's extension changes
          // and generated suffixes. Keep the upload filename unchanged.
          const stem = name.slice(0, -extname(name).length);
          variables[`q${index}`] = `filename:${stem}*`;
          return `f${index}: files(first: 2, query: $q${index}) { nodes { ${FILE_FIELDS} } }`;
        })
        .join("\n");
      const data = await request<Record<string, { nodes: File[] }>>(
        `#graphql\nquery SourceImageFiles(${declarations}) { ${fields} }`,
        variables,
        250,
        guard,
      );
      batch.forEach((name, index) => {
        const page = data[`f${index}`];
        if (!page || page.nodes.length > 1)
          throw new Error(`Invalid source image lookup: ${name}`);
        if (page.nodes[0]) files.set(name, page.nodes[0]);
      });
    }
    const missing = allNames.filter(
      (name) =>
        !files.has(name) ||
        (!knownFiles.has(name) &&
          (files.get(name)!.fileStatus === "FAILED" ||
            files.get(name)!.status === "FAILED")),
    );
    // Check only files that would need an upload. Cancel the GET body after
    // headers: Shopify still downloads the actual image directly from the CDN.
    const uploadable = new Set<string>();
    let nextCheck = 0;
    const checkNext = async () => {
      while (nextCheck < missing.length) {
        const name = missing[nextCheck++];
        let failure = "Source image could not be reached.";
        let usable = false;
        for (let attempt = 0; attempt < 3; attempt++) {
          // Ownership errors must stop the worker, never become image skips.
          guard();
          let retryable = false;
          try {
            const response = await fetch(entries.get(name)!.url, {
              signal: AbortSignal.timeout(30_000),
            });
            const type = response.headers.get("content-type") ?? "";
            usable = response.ok && !type.toLowerCase().includes("text/html");
            failure = `Source image returned HTTP ${response.status}${type ? ` (${type})` : ""}.`;
            retryable =
              response.status === 408 ||
              response.status === 429 ||
              response.status >= 500;
            // A body cancellation error does not invalidate successful headers.
            await response.body?.cancel().catch(() => undefined);
          } catch (error) {
            retryable = true;
            const detail =
              error instanceof Error ? error.message : "Unknown network error";
            const cause = error instanceof Error ? error.cause : undefined;
            const code =
              cause && typeof cause === "object" && "code" in cause
                ? String(cause.code)
                : "";
            failure = `Source image could not be reached: ${detail}${code ? ` (${code})` : ""}.`;
          }
          guard();
          if (usable || !retryable) break;
          if (attempt < 2) await delay(1000 * (attempt + 1));
        }
        if (usable) {
          uploadable.add(name);
          continue;
        }
        files.set(name, {
          __typename: "MediaImage",
          id: "",
          fileStatus: "FAILED",
          status: "FAILED",
          fileErrors: [{ code: "SOURCE_UNAVAILABLE", message: failure }],
        });
      }
    };
    await Promise.all(
      Array.from({ length: Math.min(3, missing.length) }, checkNext),
    );
    const uploads = missing.filter((name) => uploadable.has(name));
    for (let offset = 0; offset < uploads.length; offset += 250) {
      const batch = uploads.slice(offset, offset + 250);
      const created = await request<{
        fileCreate: { files: File[]; userErrors: Errors };
      }>(
        CREATE,
        {
          files: batch.map((name) => ({
            filename: name,
            originalSource: entries.get(name)!.url,
            contentType: "IMAGE",
            // A failed upload can reserve a name without appearing in search.
            // URL-hashed names let a retry replace that reservation safely.
            duplicateResolutionMode: "REPLACE",
            alt: entries.get(name)!.title,
          })),
        },
        20,
        guard,
      );
      assertNoErrors(created.fileCreate.userErrors);
      if (created.fileCreate.files.length !== batch.length)
        throw new Error(
          "Shopify did not confirm every image upload. Resume this job to recover existing files.",
        );
      batch.forEach((name, index) =>
        files.set(name, created.fileCreate.files[index]),
      );
    }
    for (const [name, file] of files) knownFiles.set(name, file);
    return { entries, names, files, allNames };
  };
  const syncGroup = async (
    products: SourceProduct[],
    guard: () => void = () => {},
  ) => {
    if (!products.length) return;
    if (products.length > 25)
      throw new Error("Sync at most 25 image products per group.");
    const prepared = await prepareGroup(products, guard);
    if (!prepared) return;
    const { entries, names, files, allNames } = prepared;
    const existing = await lookup(products, guard);
    const waitForFiles = async () => {
      for (let attempt = 0; attempt < 46; attempt++) {
        guard();
        const pending: string[] = [];
        for (const name of allNames) {
          const file = files.get(name)!;
          if (file.__typename !== "MediaImage")
            throw new Error(
              `Source image resolved to a different file type: ${name}`,
            );
          if (file.fileStatus === "FAILED" || file.status === "FAILED")
            continue;
          if (
            file.fileStatus !== "READY" ||
            (file.status && file.status !== "READY") ||
            !file.image?.url
          )
            pending.push(file.id);
          else {
            // Track processing by Shopify file ID. The delivery URL is not a
            // reliable way to verify the original upload filename.
            knownFiles.set(name, file);
          }
        }
        if (!pending.length) break;
        if (attempt % 5 === 0)
          console.log(
            `Images: waiting for ${pending.length} Shopify files to finish processing.`,
          );
        if (attempt === 45) break;
        if (attempt > 0) await delay(2000);
        for (let offset = 0; offset < pending.length; offset += 250) {
          const ids = pending.slice(offset, offset + 250);
          const checked = await request<{ nodes: (File | null)[] }>(
            CHECK,
            { ids },
            ids.length * 3 + 1,
            guard,
          );
          const byId = new Map(
            checked.nodes
              .filter((file): file is File => Boolean(file))
              .map((file) => [file.id, file]),
          );
          if (ids.some((id) => !byId.has(id)))
            throw new Error(
              "A source image file disappeared during processing.",
            );
          for (const [name, file] of files)
            if (byId.has(file.id)) files.set(name, byId.get(file.id)!);
        }
      }
    };
    // Retry only Shopify's explicit download failures, never invalid image
    // formats/sizes or files that are still processing. Keep the filename stable.
    for (;;) {
      await waitForFiles();
      const retryNames = allNames.filter((name) => {
        const file = files.get(name)!;
        return (
          Boolean(file.id) &&
          (file.fileStatus === "FAILED" || file.status === "FAILED") &&
          file.fileErrors.some(
            (error) => error.code === "IMAGE_DOWNLOAD_FAILURE",
          ) &&
          (downloadRetries.get(name) ?? 0) < 2
        );
      });
      if (!retryNames.length) break;
      guard();
      const retrySet = new Set(retryNames);
      const retryProducts = products
        .map((product) => ({
          ...product,
          imageUrls: product.imageUrls.filter((url) =>
            retrySet.has(sourceImageFilename(url)),
          ),
        }))
        .filter((product) => product.imageUrls.length);
      const nextAttempt = Math.max(
        ...retryNames.map((name) => (downloadRetries.get(name) ?? 0) + 1),
      );
      await delay(2000 * nextAttempt);
      guard();
      for (const name of retryNames) {
        downloadRetries.set(name, (downloadRetries.get(name) ?? 0) + 1);
        knownFiles.delete(name);
      }
      console.log(
        `Images: retrying ${retryNames.length} Shopify download failures.`,
      );
      const retried = await prepareGroup(retryProducts, guard);
      if (!retried)
        throw new Error("No images were prepared for download recovery.");
      for (const name of retryNames) {
        const replacement = retried.files.get(name);
        if (!replacement)
          throw new Error("Shopify image retry was not confirmed.");
        files.set(name, replacement);
      }
    }
    const ready = (name: string) => {
      const file = files.get(name)!;
      return (
        file.fileStatus === "READY" &&
        (!file.status || file.status === "READY") &&
        Boolean(file.image?.url)
      );
    };
    const desiredByProduct = names.map((list, i) => {
      for (const name of list) {
        const file = files.get(name)!;
        knownFiles.set(name, file);
        if (!ready(name))
          console.warn(
            `Image skipped for ${products[i].storeId}:${products[i].productId}: ` +
              `${entries.get(name)!.url} (${file.fileStatus}; ` +
              `${file.fileErrors.map((error) => error.message).join("; ") || "not ready"}).`,
          );
      }
      // Preserve all current images if the requested main image is unavailable.
      // A later import retries the source; existing products keep their images.
      if (!ready(list[0])) return null;
      return list.filter(ready).map((name) => files.get(name)!.id);
    });
    let nextProduct = 0;
    let firstError: unknown;
    const syncNext = async () => {
      while (!firstError && nextProduct < products.length) {
        const i = nextProduct++;
        try {
          const desired = desiredByProduct[i];
          if (!desired) continue;
          if (sameIds(existing[i].mediaIds, desired)) continue;
          const saved = await request<{
            productSet: {
              product: {
                id: string;
                media: { nodes: { id: string }[] };
              } | null;
              userErrors: Errors;
            };
          }>(
            ATTACH,
            {
              count: desired.length,
              identifier: { id: existing[i].id },
              input: { files: desired.map((id) => ({ id })) },
            },
            20,
            guard,
          );
          assertNoErrors(saved.productSet.userErrors);
          if (saved.productSet.product?.id !== existing[i].id)
            throw new Error("Shopify returned an unexpected image product.");
          if (
            sameIds(
              saved.productSet.product.media.nodes.map((media) => media.id),
              desired,
            )
          )
            continue;
          const ordered = await request<{
            productReorderMedia: {
              job: { id: string } | null;
              mediaUserErrors: Errors;
            };
          }>(
            ORDER,
            {
              id: existing[i].id,
              moves: desired.map((id, index) => ({
                id,
                newPosition: String(index),
              })),
            },
            20,
            guard,
          );
          assertNoErrors(ordered.productReorderMedia.mediaUserErrors);
          if (!ordered.productReorderMedia.job)
            throw new Error("Shopify did not confirm image ordering.");
          const jobId = ordered.productReorderMedia.job.id;
          let done = false;
          for (let poll = 0; poll < 46; poll++) {
            const result = await request<{
              job: { id: string; done: boolean } | null;
            }>(JOB, { id: jobId }, 5, guard);
            if (!result.job || result.job.id !== jobId)
              throw new Error("Shopify image ordering job disappeared.");
            if (result.job.done) {
              done = true;
              break;
            }
            await delay(2000);
          }
          if (!done)
            throw new Error(
              "Shopify is still ordering images. Resume this import later.",
            );
        } catch (error) {
          firstError ??= error;
        }
      }
    };
    await Promise.all(
      Array.from({ length: Math.min(3, products.length) }, () => syncNext()),
    );
    if (firstError) throw firstError;
    const verified = await lookup(products, guard);
    if (
      verified.some(
        (product, i) =>
          desiredByProduct[i] !== null &&
          !sameIds(product.mediaIds, desiredByProduct[i]!),
      )
    )
      throw new Error(
        "Shopify has not applied the expected image list yet. Resume the import.",
      );
  };
  return {
    prepareGroup,
    syncGroup,
    get requestCount() {
      return api.requestCount;
    },
  };
}
