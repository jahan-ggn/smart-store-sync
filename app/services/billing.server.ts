import process from "node:process";

import type { createImportAdmin } from "./import-admin.server";

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
