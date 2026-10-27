import process from "node:process";
import { Buffer } from "node:buffer";
import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { mkdir, mkdtemp, open, rm } from "node:fs/promises";
import { join, resolve } from "node:path";
import db from "../db.server";
import {
  checkImportShop,
  ImportJobConflict,
  prepareImportJob,
} from "./import-jobs.server";
import {
  requireSourceSubscription,
  SourceSubscriptionError,
} from "./source-subscription.server";

const MAX_BYTES = 50 * 1024 * 1024;
class IntakeError extends Error {
  constructor(
    message: string,
    public readonly status: number,
  ) {
    super(message);
  }
}
export type DeliveryHeaders = {
  shop: string;
  id: string;
  generatedAt: Date;
  hash: string;
};
export function verifyDeliveryHeaders(request: Request): DeliveryHeaders {
  const secret = process.env.SOURCE_WEBHOOK_SECRET ?? "";
  if (!/^[a-f0-9]{64}$/.test(secret))
    throw new IntakeError(
      "Source delivery authentication is not configured.",
      503,
    );
  const shop = request.headers.get("X-Source-Shop") ?? "";
  try {
    checkImportShop(shop);
  } catch {
    throw new IntakeError("Invalid shop.", 400);
  }
  const id = request.headers.get("X-Source-Delivery-Id") ?? "";
  const timestamp = request.headers.get("X-Source-Timestamp") ?? "";
  const generated = request.headers.get("X-Source-Generated-At") ?? "";
  const hash = request.headers.get("X-Source-Content-SHA256") ?? "";
  const signature = request.headers.get("X-Source-Signature") ?? "";
  if (
    !/^[a-f0-9-]{36}$/.test(id) ||
    !/^\d{10}$/.test(timestamp) ||
    !/^\d{13}$/.test(generated) ||
    !/^[a-f0-9]{64}$/.test(hash) ||
    !/^[a-f0-9]{64}$/.test(signature)
  )
    throw new IntakeError("Invalid source delivery headers.", 401);
  if (Math.abs(Date.now() - Number(timestamp) * 1000) > 300_000)
    throw new IntakeError("The source delivery signature has expired.", 401);
  if (
    Number(generated) > Date.now() + 300_000 ||
    Number(generated) < Date.now() - 7 * 86400_000
  )
    throw new IntakeError(
      "Source generation time is outside the accepted window.",
      400,
    );
  const message = [
    "v1",
    "POST",
    "/api/source/imports",
    shop,
    id,
    timestamp,
    generated,
    hash,
  ].join("\n");
  const expected = createHmac("sha256", Buffer.from(secret, "hex"))
    .update(message)
    .digest();
  if (!timingSafeEqual(expected, Buffer.from(signature, "hex")))
    throw new IntakeError("Invalid source delivery signature.", 401);
  return { shop, id, hash, generatedAt: new Date(Number(generated)) };
}
async function existingDelivery(delivery: DeliveryHeaders) {
  const saved = await db.importJob.findUnique({
    where: {
      shop_sourceDeliveryId: {
        shop: delivery.shop,
        sourceDeliveryId: delivery.id,
      },
    },
  });
  if (!saved) return null;
  if (
    (saved.fileHash && saved.fileHash !== delivery.hash) ||
    saved.sourceGeneratedAt?.getTime() !== delivery.generatedAt.getTime()
  )
    throw new IntakeError(
      "This delivery ID was already used with different contents.",
      409,
    );
  if (saved.status === "preparing")
    throw new IntakeError(
      "This delivery is still being prepared. Retry later with the same ID.",
      409,
    );
  if (["failed", "cancelled"].includes(saved.status))
    throw new IntakeError(
      "This delivery failed or was cancelled. Send a fresh delivery after resolving the problem.",
      422,
    );
  return Response.json({
    success: true,
    duplicate: true,
    jobId: saved.id,
    status: saved.status,
  });
}
async function saveUpload(
  request: Request,
  path: string,
  expectedHash: string,
) {
  if (!request.body)
    throw new IntakeError("A CSV request body is required.", 400);
  const length = request.headers.get("Content-Length");
  if (length && (!/^\d+$/.test(length) || Number(length) > MAX_BYTES))
    throw new IntakeError("CSV exceeds the 50 MiB upload limit.", 413);
  if (
    !(request.headers.get("Content-Type") ?? "")
      .toLowerCase()
      .startsWith("text/csv")
  )
    throw new IntakeError(
      "Send the raw CSV body with Content-Type: text/csv.",
      415,
    );
  const reader = request.body.getReader();
  const file = await open(path, "wx", 0o600);
  const digest = createHash("sha256");
  let size = 0;
  let timedOut = false;
  const cancel = () => {
    void reader.cancel().catch(() => undefined);
  };
  const timer = setTimeout(() => {
    timedOut = true;
    cancel();
  }, 60_000);
  request.signal.addEventListener("abort", cancel, { once: true });
  try {
    request.signal.throwIfAborted();
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      size += chunk.value.byteLength;
      if (size > MAX_BYTES)
        throw new IntakeError("CSV exceeds the 50 MiB upload limit.", 413);
      digest.update(chunk.value);
      await file.writeFile(chunk.value);
    }
    request.signal.throwIfAborted();
    if (timedOut) throw new IntakeError("CSV upload timed out.", 408);
    if (!size) throw new IntakeError("The CSV body is empty.", 400);
    if (digest.digest("hex") !== expectedHash)
      throw new IntakeError("CSV checksum does not match its signature.", 400);
  } finally {
    clearTimeout(timer);
    request.signal.removeEventListener("abort", cancel);
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
    await file.close();
  }
}
export async function receiveSourceCsv(request: Request) {
  let directory: string | undefined;
  try {
    if (request.method !== "POST")
      return Response.json(
        { error: "Use POST." },
        { status: 405, headers: { Allow: "POST" } },
      );
    const delivery = verifyDeliveryHeaders(request);
    const subscription = await requireSourceSubscription(delivery.shop);
    const session = await db.session.findFirst({
      where: { shop: delivery.shop, isOnline: false },
      select: { id: true },
    });
    if (!session)
      throw new IntakeError(
        "The Shopify app is not connected to this shop.",
        403,
      );
    const account = await db.accountSettings.findUnique({
      where: { shop: delivery.shop },
      select: { inventoryLocationId: true },
    });
    if (!account?.inventoryLocationId)
      throw new IntakeError(
        "Configure this shop's inventory location before automatic imports.",
        422,
      );
    const duplicate = await existingDelivery(delivery);
    if (duplicate) return duplicate;
    if (await db.importJob.findUnique({ where: { activeShop: delivery.shop } }))
      throw new IntakeError(
        "This shop has an active import. Retry this delivery later.",
        409,
      );
    const incoming = resolve("var/imports/incoming");
    await mkdir(incoming, { recursive: true });
    directory = await mkdtemp(join(incoming, "upload-"));
    const path = join(directory, "source.csv");
    await saveUpload(request, path, delivery.hash);
    let job;
    try {
      job = await prepareImportJob(path, delivery.shop, {
        id: delivery.id,
        generatedAt: delivery.generatedAt,
        storeIds: subscription.storeIds,
      });
    } catch (error) {
      const repeated = await existingDelivery(delivery);
      if (repeated) return repeated;
      if (error instanceof ImportJobConflict)
        throw new IntakeError(error.message, 409);
      throw new IntakeError(
        "CSV preparation failed. Inspect the import job errors before sending a corrected delivery.",
        422,
      );
    }
    return Response.json(
      { success: true, duplicate: false, jobId: job.id, status: job.status },
      { status: 202 },
    );
  } catch (error) {
    const status =
      error instanceof IntakeError || error instanceof SourceSubscriptionError
        ? error.status
        : 503;
    const message =
      error instanceof IntakeError || error instanceof SourceSubscriptionError
        ? error.message
        : "Unable to accept this delivery. Retry later.";
    return Response.json(
      { success: false, error: message },
      {
        status,
        headers:
          status === 409 || status === 503
            ? { "Retry-After": "60" }
            : undefined,
      },
    );
  } finally {
    if (directory) await rm(directory, { recursive: true, force: true });
  }
}
