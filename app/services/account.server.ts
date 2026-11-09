import { Buffer } from "node:buffer";
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import process from "node:process";

import db from "../db.server";

export class AccountError extends Error {}

type Settings = {
  defaultMargin: string;
  minPrice: string;
  maxPrice: string;
  skipUnmapped: boolean;
};

const defaults: Settings = {
  defaultMargin: "0",
  minPrice: "0",
  maxPrice: "0",
  skipUnmapped: true,
};

function encryptionKey() {
  const key = Buffer.from(
    process.env.SOURCE_TOKEN_ENCRYPTION_KEY ?? "",
    "base64",
  );

  if (key.length !== 32) {
    throw new Error("Invalid purchase-token encryption key");
  }

  return key;
}

function encryptToken(token: string, shop: string) {
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", encryptionKey(), iv);

  cipher.setAAD(Buffer.from(shop, "utf8"));

  const encrypted = Buffer.concat([
    cipher.update(token, "utf8"),
    cipher.final(),
  ]);

  return [
    "v1",
    iv.toString("base64"),
    cipher.getAuthTag().toString("base64"),
    encrypted.toString("base64"),
  ].join(".");
}

function decryptToken(value: string, shop: string) {
  const parts = value.split(".");

  if (parts.length !== 4 || parts[0] !== "v1") {
    throw new Error("Invalid encrypted token format");
  }

  const iv = Buffer.from(parts[1]!, "base64");
  const tag = Buffer.from(parts[2]!, "base64");
  const encrypted = Buffer.from(parts[3]!, "base64");

  if (iv.length !== 12 || tag.length !== 16) {
    throw new Error("Invalid encrypted token");
  }

  const decipher = createDecipheriv("aes-256-gcm", encryptionKey(), iv);

  decipher.setAAD(Buffer.from(shop, "utf8"));
  decipher.setAuthTag(tag);

  return Buffer.concat([decipher.update(encrypted), decipher.final()]).toString(
    "utf8",
  );
}

function readText(form: FormData, name: string) {
  const value = form.get(name);
  return typeof value === "string" ? value.trim() : "";
}

function readAmount(form: FormData, name: string, label: string) {
  const value = readText(form, name);

  if (!/^\d{1,10}$/.test(value)) {
    throw new AccountError(`${label} must be a non-negative whole number.`);
  }

  return BigInt(value).toString();
}

export async function loadAccount(shop: string) {
  const saved = await db.accountSettings.findUnique({
    where: { shop },
  });

  return {
    settings: saved
      ? {
          defaultMargin: saved.defaultMargin.replace(/\.0+$/, ""),
          minPrice: saved.minPrice.replace(/\.0+$/, ""),
          maxPrice: saved.maxPrice.replace(/\.0+$/, ""),
          skipUnmapped: saved.skipUnmapped,
        }
      : { ...defaults },
    hasToken: Boolean(saved),
  };
}

export async function saveAccount(shop: string, form: FormData) {
  const settings: Settings = {
    defaultMargin: readAmount(form, "defaultMargin", "Default profit margin"),
    minPrice: readAmount(form, "minPrice", "Minimum price"),
    maxPrice: readAmount(form, "maxPrice", "Maximum price"),
    skipUnmapped: readText(form, "skipUnmapped") === "true",
  };

  const min = BigInt(settings.minPrice);
  const max = BigInt(settings.maxPrice);

  if (max !== 0n && min > max) {
    throw new AccountError(
      "Maximum price must be at least the minimum price, or 0 to disable.",
    );
  }

  const saved = await db.accountSettings.findUnique({
    where: { shop },
    select: { shop: true },
  });

  if (!saved) {
    throw new AccountError(
      "Your source account is not connected. Reload Account & Plan to retry setup.",
    );
  }

  // Settings forms never create or replace source credentials.
  await db.accountSettings.update({
    where: { shop },
    data: settings,
  });
}

export async function getSavedPurchaseToken(shop: string) {
  const saved = await db.accountSettings.findUnique({
    where: { shop },
  });

  return saved ? decryptToken(saved.tokenEncrypted, shop) : null;
}

export class SourceProvisioningError extends Error {}

function provisioningRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// Call only after Shopify authentication, using session.shop from the server.
export async function ensureSourceAccount(shop: string): Promise<void> {
  if (!/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.myshopify\.com$/.test(shop)) {
    throw new SourceProvisioningError("Invalid Shopify shop domain.");
  }

  if (await getSavedPurchaseToken(shop)) return;

  // Check encryption configuration before creating a backend record.
  encryptionKey();

  const base = process.env.SOURCE_API_BASE_URL?.replace(/\/+$/, "");
  const key = process.env.SHOPIFY_PROVISIONING_API_KEY ?? "";

  if (!base || key.length < 32) {
    throw new SourceProvisioningError("Source provisioning is not configured.");
  }

  let endpoint: URL;

  try {
    endpoint = new URL(`${base}/subscriptions/shopify/provision`);
  } catch {
    throw new SourceProvisioningError("Invalid source service URL.");
  }

  const local = ["localhost", "127.0.0.1", "[::1]"].includes(endpoint.hostname);

  if (
    endpoint.username ||
    endpoint.password ||
    endpoint.search ||
    endpoint.hash ||
    (endpoint.protocol !== "https:" &&
      !(
        endpoint.protocol === "http:" &&
        local &&
        process.env.NODE_ENV !== "production"
      ))
  ) {
    throw new SourceProvisioningError("The source service requires HTTPS.");
  }

  let result: unknown;

  try {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Provisioning-Key": key,
      },
      body: JSON.stringify({ shop }),
      signal: AbortSignal.timeout(15_000),
      redirect: "error",
      cache: "no-store",
    });

    if (!response.ok) throw new SourceProvisioningError();

    result = await response.json();
  } catch {
    throw new SourceProvisioningError(
      "Unable to connect your source account. Please try again.",
    );
  }

  if (
    !provisioningRecord(result) ||
    result.success !== true ||
    !provisioningRecord(result.data) ||
    result.data.shop !== shop ||
    result.data.buyer_domain !== `https://${shop}` ||
    result.data.platform !== "shopify" ||
    !Number.isSafeInteger(result.data.subscription_id) ||
    (result.data.subscription_id as number) < 1 ||
    typeof result.data.token !== "string" ||
    !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(
      result.data.token,
    )
  ) {
    throw new SourceProvisioningError("Invalid source provisioning response.");
  }

  const tokenEncrypted = encryptToken(result.data.token, shop);

  await db.accountSettings.upsert({
    where: { shop },
    create: { shop, tokenEncrypted, ...defaults },

    // Preserve credentials and settings if another request saved them first.
    update: { shop },
  });
}
