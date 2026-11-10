import process from "node:process";

import type { createImportAdmin } from "./import-admin.server";
import { ensureSourceAccount, getSavedPurchaseToken } from "./account.server";

export class BillingUnavailable extends Error {
  constructor() {
    super("Unable to verify Shopify billing. Please try again shortly.");
    this.name = "BillingUnavailable";
  }
}

export type BillingState = {
  status: "inactive" | "trial" | "active";
  expiresAt: string | null;
  cancelAtEndOfCycle: boolean;
};

const identityQuery = `query BillingIdentity {
  shop { id myshopifyDomain }
  currentAppInstallation { app { id apiKey } }
}`;

const subscriptionQuery = `query BillingSubscription($appId: ID!, $shopId: ID!) {
  activeSubscription(appId: $appId, shopId: $shopId) {
    app { id }
    shop { id myshopifyDomain }
    billingPeriod
    trialEndsAt
    currentBillingCycle { endTime }
    cancelAtEndOfCycle
    items { handle }
  }
}`;

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function graphqlData(value: unknown): Record<string, unknown> {
  if (
    !record(value) ||
    (value.errors !== undefined &&
      (!Array.isArray(value.errors) || value.errors.length > 0)) ||
    !record(value.data)
  ) {
    throw new BillingUnavailable();
  }

  return value.data;
}

// Use session.shop and the authenticated Admin client, never browser identity.
// Read-only: this does not provision accounts or update backend expiry.
export async function readShopifyBilling(
  shop: string,
  admin: ReturnType<typeof createImportAdmin>,
): Promise<BillingState> {
  try {
    const partnerId = process.env.SHOPIFY_PARTNER_ID?.trim() ?? "";
    const partnerToken = process.env.SHOPIFY_PARTNER_API_TOKEN?.trim() ?? "";
    const apiKey = process.env.SHOPIFY_API_KEY;

    if (
      !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.myshopify\.com$/.test(shop) ||
      !/^[1-9]\d*$/.test(partnerId) ||
      !partnerToken ||
      !apiKey
    ) {
      throw new BillingUnavailable();
    }

    const identityResponse = await admin.graphql(identityQuery, {
      signal: AbortSignal.timeout(15_000),
    });

    if (!identityResponse.ok) {
      throw new BillingUnavailable();
    }

    const identity = graphqlData(await identityResponse.json());
    const savedShop = identity.shop;
    const installation = identity.currentAppInstallation;
    const app = record(installation) ? installation.app : null;

    if (
      !record(savedShop) ||
      !record(app) ||
      savedShop.myshopifyDomain !== shop ||
      app.apiKey !== apiKey ||
      typeof savedShop.id !== "string" ||
      !/^gid:\/\/shopify\/Shop\/[1-9]\d*$/.test(savedShop.id) ||
      typeof app.id !== "string" ||
      !/^gid:\/\/shopify\/App\/[1-9]\d*$/.test(app.id)
    ) {
      throw new BillingUnavailable();
    }

    const response = await fetch(
      `https://partners.shopify.com/${partnerId}/api/2026-07/graphql.json`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Shopify-Access-Token": partnerToken,
        },
        body: JSON.stringify({
          query: subscriptionQuery,
          variables: {
            appId: app.id,
            shopId: savedShop.id,
          },
        }),
        signal: AbortSignal.timeout(15_000),
        redirect: "error",
        cache: "no-store",
      },
    );

    if (!response.ok) {
      throw new BillingUnavailable();
    }

    const subscription = graphqlData(await response.json()).activeSubscription;

    if (subscription === null) {
      return {
        status: "inactive",
        expiresAt: null,
        cancelAtEndOfCycle: false,
      };
    }

    if (
      !record(subscription) ||
      !record(subscription.app) ||
      subscription.app.id !== app.id ||
      !record(subscription.shop) ||
      subscription.shop.id !== savedShop.id ||
      subscription.shop.myshopifyDomain !== shop ||
      subscription.billingPeriod !== "EVERY_30_DAYS" ||
      typeof subscription.cancelAtEndOfCycle !== "boolean" ||
      !Array.isArray(subscription.items) ||
      subscription.items.length !== 1 ||
      !record(subscription.items[0]) ||
      subscription.items[0].handle !== "standard"
    ) {
      throw new BillingUnavailable();
    }

    const trial = subscription.trialEndsAt !== null;
    const cycle = subscription.currentBillingCycle;

    // Shopify returns a trial end OR a billing cycle.
    if (trial === (cycle !== null)) {
      throw new BillingUnavailable();
    }

    const expiry = trial
      ? subscription.trialEndsAt
      : record(cycle)
        ? cycle.endTime
        : undefined;

    if (
      typeof expiry !== "string" ||
      !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(
        expiry,
      ) ||
      !Number.isFinite(Date.parse(expiry))
    ) {
      throw new BillingUnavailable();
    }

    return {
      status:
        Date.parse(expiry) <= Date.now()
          ? "inactive"
          : trial
            ? "trial"
            : "active",
      expiresAt: new Date(expiry).toISOString(),
      cancelAtEndOfCycle: subscription.cancelAtEndOfCycle,
    };
  } catch {
    // Never expose raw API errors, headers, response bodies or credentials.
    throw new BillingUnavailable();
  }
}

async function writeSourceBilling(
  shop: string,
  token: string,
  expiresAt: string | null,
  checkedAt: string,
): Promise<boolean> {
  try {
    const base = process.env.SOURCE_API_BASE_URL?.replace(/\/+$/, "");
    const key = process.env.SHOPIFY_PROVISIONING_API_KEY ?? "";

    if (!base || key.length < 32) {
      throw new BillingUnavailable();
    }

    const endpoint = new URL(`${base}/subscriptions/shopify/billing`);
    const local = ["localhost", "127.0.0.1", "[::1]"].includes(
      endpoint.hostname,
    );

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
      throw new BillingUnavailable();
    }

    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Provisioning-Key": key,
      },
      body: JSON.stringify({
        shop,
        token,
        expires_at: expiresAt,
        checked_at: checkedAt,
      }),
      signal: AbortSignal.timeout(15_000),
      redirect: "error",
      cache: "no-store",
    });

    if (!response.ok) {
      throw new BillingUnavailable();
    }

    const result: unknown = await response.json();

    if (
      !record(result) ||
      result.success !== true ||
      !record(result.data) ||
      result.data.shop !== shop ||
      !Number.isSafeInteger(result.data.subscription_id) ||
      (result.data.subscription_id as number) < 1 ||
      typeof result.data.expires_at !== "string" ||
      (result.data.stale !== undefined &&
        typeof result.data.stale !== "boolean")
    ) {
      throw new BillingUnavailable();
    }
    return result.data.stale !== true;
  } catch {
    throw new BillingUnavailable();
  }
}

export async function syncShopifyBilling(
  shop: string,
  admin: ReturnType<typeof createImportAdmin>,
): Promise<BillingState> {
  const checkedAt = new Date().toISOString();
  const billing = await readShopifyBilling(shop, admin);

  try {
    await ensureSourceAccount(shop);

    const token = await getSavedPurchaseToken(shop);
    if (!token) throw new BillingUnavailable();

    const current = await writeSourceBilling(
      shop,
      token,
      billing.status === "inactive" ? null : billing.expiresAt,
      checkedAt,
    );

    if (!current) throw new BillingUnavailable();

    return billing;
  } catch {
    throw new BillingUnavailable();
  }
}

// Call only after authenticating an uninstall webhook.
// Preserve the original event time when retrying the same uninstall.
export async function revokeSourceBilling(
  shop: string,
  triggeredAt: string,
): Promise<boolean> {
  try {
    if (
      !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.myshopify\.com$/.test(shop) ||
      !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(
        triggeredAt,
      ) ||
      !Number.isFinite(Date.parse(triggeredAt))
    ) {
      throw new BillingUnavailable();
    }

    const token = await getSavedPurchaseToken(shop);

    // No backend account exists, but local uninstall cleanup can proceed.
    if (!token) return true;

    return await writeSourceBilling(shop, token, null, triggeredAt);
  } catch {
    throw new BillingUnavailable();
  }
}
