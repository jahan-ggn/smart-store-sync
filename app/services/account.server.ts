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

  const suppliedToken = readText(form, "purchase_token");

  if (suppliedToken.length > 256) {
    throw new AccountError("Enter a valid purchase token.");
  }

  let token = suppliedToken;

  if (!token) {
    const saved = await db.accountSettings.findUnique({
      where: { shop },
    });

    if (saved) {
      token = decryptToken(saved.tokenEncrypted, shop);
    }
  }

  if (!token) {
    throw new AccountError("Enter your purchase token.");
  }

  const values = {
    ...settings,
    tokenEncrypted: encryptToken(token, shop),
  };

  await db.accountSettings.upsert({
    where: { shop },
    create: { shop, ...values },
    update: values,
  });
}

export async function getSavedPurchaseToken(shop: string) {
  const saved = await db.accountSettings.findUnique({
    where: { shop },
  });

  return saved ? decryptToken(saved.tokenEncrypted, shop) : null;
}
