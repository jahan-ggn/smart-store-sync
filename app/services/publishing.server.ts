import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import type { Prisma } from "@prisma/client";
import db from "../db.server";
import {
  checkImportShop,
  hashImportFile,
  IMPORT_BATCH_SIZE,
} from "./import-jobs.server";
import { createImportRules } from "./import-rules.server";
import { readSourceCsv, type SourceProduct } from "./source-csv.server";
import { createRequester } from "./shopify-product.server";

type Admin = Parameters<typeof createRequester>[0];
type Snapshot = {
  settings: Parameters<typeof createImportRules>[0];
  mappings: Parameters<typeof createImportRules>[1];
};
type PageInfo = { hasNextPage: boolean; endCursor: string | null };
type ChannelPage = { nodes: { handle: string }[]; pageInfo: PageInfo };
type Publication = { id: string; channels: ChannelPage };
type Product = {
  id: string;
  status: string;
  publishedOnPublication: boolean;
  media: { nodes: { id: string; status: string }[] };
};
type UserErrors = { field: string[] | null; message: string }[];
const LEASE_MS = 120_000;
const PUBLICATIONS = `#graphql
  query ImportPublications($after: String) {
    publications(first: 25, after: $after, catalogType: APP) {
      nodes {
        id
        channels(first: 10) {
          nodes { handle }
          pageInfo { hasNextPage endCursor }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
`;
const MORE_CHANNELS = `#graphql
  query ImportPublicationChannels($id: ID!, $after: String!) {
    publication(id: $id) {
      id
      channels(first: 50, after: $after) {
        nodes { handle }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
`;

export function createProductPublisher(admin: Admin) {
  const api = createRequester(admin);
  async function onlineStorePublication(guard: () => void) {
    const matches = new Set<string>();
    const cursors = new Set<string>();
    let after: string | null = null;
    do {
      const result: {
        publications: { nodes: Publication[]; pageInfo: PageInfo };
      } = await api.request(PUBLICATIONS, { after }, 400, guard);
      for (const publication of result.publications.nodes) {
        let channels = publication.channels;
        const channelCursors = new Set<string>();
        for (;;) {
          if (
            channels.nodes.some((channel) => channel.handle === "online_store")
          )
            matches.add(publication.id);
          if (!channels.pageInfo.hasNextPage) break;
          const cursor = channels.pageInfo.endCursor;
          if (!cursor || channelCursors.has(cursor))
            throw new Error("Invalid publication channel cursor.");
          channelCursors.add(cursor);
          const more = await api.request<{ publication: Publication | null }>(
            MORE_CHANNELS,
            { id: publication.id, after: cursor },
            70,
            guard,
          );
          if (!more.publication || more.publication.id !== publication.id)
            throw new Error("The publishing destination disappeared.");
          channels = more.publication.channels;
        }
      }
      if (!result.publications.pageInfo.hasNextPage) break;
      after = result.publications.pageInfo.endCursor;
      if (!after || cursors.has(after))
        throw new Error("Invalid publication cursor.");
      cursors.add(after);
    } while (after);
    if (matches.size !== 1)
      throw new Error(
        "Unable to identify a single Online Store publication. Check that the Online Store sales channel is enabled.",
      );
    return [...matches][0];
  }
  async function lookup(
    products: SourceProduct[],
    publicationId: string,
    guard: () => void,
  ) {
    const variables: Record<string, unknown> = { publicationId };
    const declarations = products.map(
      (_, i) => `$p${i}: ProductIdentifierInput!`,
    );
    const fields = products.map((product, i) => {
      variables[`p${i}`] = {
        customId: {
          namespace: "$app",
          key: "source_product_id",
          value: `${product.storeId}:${product.productId}`,
        },
      };
      return `p${i}: productByIdentifier(identifier: $p${i}) {
        id status publishedOnPublication(publicationId: $publicationId)
        media(first: 1) { nodes { id status } }
      }`;
    });
    const data = await api.request<Record<string, Product | null>>(
      `#graphql\nquery ImportPublishingProducts($publicationId: ID!, ${declarations.join(", ")}) { ${fields.join("\n")} }`,
      variables,
      200,
      guard,
    );
    return products.map((product, i) => {
      const saved = data[`p${i}`];
      if (!saved)
        throw new Error(
          `Imported product ${product.storeId}:${product.productId} is missing. Prepare a fresh import.`,
        );
      if (!saved.media.nodes.length || saved.media.nodes[0].status !== "READY")
        throw new Error(
          `Product ${saved.id} has no ready main image. Repair its images before publishing.`,
        );
      return saved;
    });
  }
  async function publishGroup(
    products: SourceProduct[],
    publicationId: string,
    guard: () => void,
  ) {
    if (!products.length) return;
    if (products.length > IMPORT_BATCH_SIZE)
      throw new Error("Publish at most 25 products per group.");
    const existing = await lookup(products, publicationId, guard);
    // Separate requests let Shopify process products concurrently rather than
    // serializing 25 root mutations inside one GraphQL request.
    let next = 0;
    let firstError: unknown;
    const publishNext = async () => {
      while (!firstError && next < existing.length) {
        const product = existing[next++];
        try {
          guard();
          if (product.status !== "ACTIVE") {
            const saved = await api.request<{
              productUpdate: {
                product: { id: string; status: string } | null;
                userErrors: UserErrors;
              };
            }>(
              `#graphql
                mutation ActivateImportProduct($product: ProductUpdateInput!) {
                  productUpdate(product: $product) {
                    product { id status }
                    userErrors { field message }
                  }
                }
              `,
              { product: { id: product.id, status: "ACTIVE" } },
              12,
              guard,
            );
            if (saved.productUpdate.userErrors.length)
              throw new Error(
                `${product.id}: ${saved.productUpdate.userErrors.map((error) => error.message).join("; ")}`,
              );
            if (
              saved.productUpdate.product?.id !== product.id ||
              saved.productUpdate.product.status !== "ACTIVE"
            )
              throw new Error(
                `Shopify did not activate product ${product.id}.`,
              );
          }
          if (!product.publishedOnPublication) {
            const saved = await api.request<{
              publishablePublish: {
                publishable: { id: string } | null;
                userErrors: UserErrors;
              };
            }>(
              `#graphql
                mutation PublishImportProduct($id: ID!, $input: [PublicationInput!]!) {
                  publishablePublish(id: $id, input: $input) {
                    publishable { ... on Product { id } }
                    userErrors { field message }
                  }
                }
              `,
              { id: product.id, input: [{ publicationId }] },
              12,
              guard,
            );
            if (saved.publishablePublish.userErrors.length)
              throw new Error(
                `${product.id}: ${saved.publishablePublish.userErrors.map((error) => error.message).join("; ")}`,
              );
            if (saved.publishablePublish.publishable?.id !== product.id)
              throw new Error(
                `Shopify did not confirm publication for ${product.id}.`,
              );
          }
        } catch (error) {
          firstError ??= error;
        }
      }
    };
    // Drain all in-flight requests before retrying or releasing worker ownership.
    await Promise.all(
      Array.from({ length: Math.min(5, existing.length) }, publishNext),
    );
    if (firstError) throw firstError;
    // Do not advance the CSV cursor until Shopify confirms both conditions.
    const verified = await lookup(products, publicationId, guard);
    if (
      verified.some(
        (product) =>
          product.status !== "ACTIVE" || !product.publishedOnPublication,
      )
    )
      throw new Error(
        "Shopify has not confirmed active Online Store publication for every product in this group. Resume this import.",
      );
  }
  return {
    onlineStorePublication,
    publishGroup,
    get requestCount() {
      return api.requestCount;
    },
  };
}

export async function publishingProgress(shop: string, jobId: string) {
  checkImportShop(shop);
  const job = await db.importJob.findFirst({ where: { id: jobId, shop } });
  if (!job) throw new Error("Import job not found for this shop.");
  return {
    jobId,
    status: job.status,
    eligible: job.eligible,
    published: job.productsPublished,
    remaining: job.eligible - job.productsPublished,
    publicationId: job.publicationId,
    lastPublishedRow: job.lastPublishedRow,
  };
}

export async function runPublishingImport(
  shop: string,
  jobId: string,
  admin: Admin,
  maxGroups = 1,
) {
  checkImportShop(shop);
  if (!Number.isSafeInteger(maxGroups) || maxGroups < 0)
    throw new Error("Invalid group limit.");
  const initial = await db.importJob.findFirst({ where: { id: jobId, shop } });
  if (!initial) throw new Error("Import job not found for this shop.");
  if (initial.status === "completed") return publishingProgress(shop, jobId);
  const token = randomUUID();
  const claimed = await db.importJob.updateMany({
    where: {
      id: jobId,
      shop,
      activeShop: shop,
      OR: [
        { status: { in: ["images_synced", "publishing_ready"] } },
        { status: "publishing_running", leaseExpiresAt: { lte: new Date() } },
      ],
    },
    data: {
      status: "publishing_running",
      workerToken: token,
      leaseExpiresAt: new Date(Date.now() + LEASE_MS),
    },
  });
  if (claimed.count !== 1)
    throw new Error(
      "Job is not ready for publishing or already has a live worker.",
    );
  const owned = {
    id: jobId,
    shop,
    activeShop: shop,
    status: "publishing_running",
    workerToken: token,
  };
  let heartbeatError: Error | undefined;
  let heartbeat = Promise.resolve();
  let heartbeatBusy = false;
  async function saveProgress(
    data: Prisma.ImportJobUpdateManyMutationInput = {},
  ) {
    if (heartbeatError) throw heartbeatError;
    const changed = await db.importJob.updateMany({
      where: owned,
      data: { leaseExpiresAt: new Date(Date.now() + LEASE_MS), ...data },
    });
    if (changed.count !== 1) throw new Error("Worker ownership was lost.");
  }
  const timer = setInterval(() => {
    if (heartbeatBusy || heartbeatError) return;
    heartbeatBusy = true;
    heartbeat = saveProgress()
      .catch((error: unknown) => {
        heartbeatError =
          error instanceof Error
            ? error
            : new Error("Unable to renew worker ownership.");
      })
      .finally(() => {
        heartbeatBusy = false;
      });
  }, 20_000);
  try {
    const job = await db.importJob.findFirstOrThrow({ where: owned });
    if (
      job.imageProductsSynced !== job.eligible ||
      job.productsSynced !== job.eligible
    )
      throw new Error(
        "Finish product, inventory and image sync before publishing.",
      );
    if (
      !job.sourcePath ||
      !job.fileHash ||
      (await hashImportFile(job.sourcePath)) !== job.fileHash
    )
      throw new Error(
        "The retained CSV is missing or changed. Cancel this job and prepare a fresh import.",
      );
    const snapshot = JSON.parse(job.ruleSnapshotJson) as Snapshot;
    const rules = createImportRules(snapshot.settings, snapshot.mappings);
    const publisher = createProductPublisher(admin);
    const guard = () => {
      if (heartbeatError) throw heartbeatError;
    };
    const publicationId = await publisher.onlineStorePublication(guard);
    if (job.publicationId && job.publicationId !== publicationId)
      throw new Error(
        "The Online Store publication changed. Prepare a fresh import.",
      );
    await saveProgress({ publicationId });
    let rowNumber = 0;
    let published = job.productsPublished;
    let groups = 0;
    let stoppedAtLimit = false;
    let pending: SourceProduct[] = [];
    const flush = async () => {
      if (!pending.length) return;
      const startedAt = performance.now();
      const requestsBefore = publisher.requestCount;
      // Retry the whole group: reread Shopify state to recover partial/lost responses.
      for (let attempt = 1; attempt <= 3; attempt++) {
        try {
          await publisher.publishGroup(pending, publicationId, guard);
          break;
        } catch (error) {
          guard();
          if (attempt === 3) throw error;
          await delay(1000 * 2 ** attempt);
        }
      }
      const nextPublished = published + pending.length;
      if (nextPublished > job.eligible)
        throw new Error("Published count exceeds the prepared product count.");
      await saveProgress({
        lastPublishedRow: rowNumber,
        productsPublished: nextPublished,
        errorsJson: "[]",
      });
      published = nextPublished;
      pending = [];
      groups++;
      console.log(
        `Publishing: ${published}/${job.eligible} products verified on Online Store. ` +
          `Group: ${((performance.now() - startedAt) / 1000).toFixed(1)}s, ` +
          `${publisher.requestCount - requestsBefore} API requests.`,
      );
    };
    for await (const row of readSourceCsv(job.sourcePath)) {
      rowNumber++;
      if (rowNumber <= job.lastPublishedRow) continue;
      if (!row.ok) throw new Error(`CSV row ${rowNumber}: ${row.error}`);
      if (rules.evaluate(row.product).eligible) pending.push(row.product);
      if (pending.length === IMPORT_BATCH_SIZE) {
        await flush();
        if (maxGroups && groups >= maxGroups) {
          stoppedAtLimit = true;
          break;
        }
      }
    }
    if (!stoppedAtLimit) {
      await flush();
      if (published !== job.eligible)
        throw new Error(
          "The CSV does not match its prepared eligible product count.",
        );
    }
    // Stop the heartbeat before the final transition releases ownership.
    clearInterval(timer);
    await heartbeat;
    const completed = published === job.eligible;
    await saveProgress({
      status: completed ? "completed" : "publishing_ready",
      ...(completed
        ? { activeShop: null, lastPublishedRow: job.rowsRead }
        : {}),
      workerToken: null,
      leaseExpiresAt: null,
      errorsJson: "[]",
    });
  } catch (error) {
    await db.importJob.updateMany({
      where: owned,
      data: {
        errorsJson: JSON.stringify([
          {
            error: (error instanceof Error
              ? error.message
              : "Publishing failed."
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
      where: { id: jobId, shop, workerToken: token },
      data: {
        status: "publishing_ready",
        workerToken: null,
        leaseExpiresAt: null,
      },
    });
  }
  return publishingProgress(shop, jobId);
}
