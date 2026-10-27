import process from "node:process";
import { getSavedPurchaseToken } from "./account.server";
import { checkImportShop } from "./import-jobs.server";

export class SourceSubscriptionError extends Error {
  constructor(
    message: string,
    public readonly status: number,
  ) {
    super(message);
  }
}
function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
export async function requireSourceSubscription(shop: string) {
  checkImportShop(shop);
  const token = await getSavedPurchaseToken(shop);
  if (!token)
    throw new SourceSubscriptionError(
      "Save the shop's purchase token first.",
      403,
    );
  const base = process.env.SOURCE_API_BASE_URL?.replace(/\/+$/, "");
  if (!base)
    throw new SourceSubscriptionError(
      "The source service is not configured.",
      503,
    );
  let response: Response;
  try {
    response = await fetch(`${base}/subscriptions/status`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token, buyer_domain: `https://${shop}` }),
      signal: AbortSignal.timeout(15_000),
      redirect: "error",
      cache: "no-store",
    });
  } catch {
    throw new SourceSubscriptionError(
      "The subscription service is unavailable.",
      503,
    );
  }
  if (response.status === 403)
    throw new SourceSubscriptionError(
      "The source subscription is inactive or its credentials were rejected.",
      403,
    );
  if (!response.ok)
    throw new SourceSubscriptionError(
      "The subscription service could not verify this shop.",
      503,
    );
  let result: unknown;
  try {
    result = await response.json();
  } catch {
    throw new SourceSubscriptionError("Invalid subscription response.", 503);
  }
  if (
    !record(result) ||
    result.success !== true ||
    !record(result.data) ||
    !Array.isArray(result.data.selected_stores) ||
    typeof result.data.expires_at !== "string"
  )
    throw new SourceSubscriptionError("Invalid subscription response.", 503);
  if (result.data.platform !== "shopify")
    throw new SourceSubscriptionError(
      "The source subscription must be registered for Shopify.",
      403,
    );
  const ids = result.data.selected_stores.map((store) => {
    if (
      !record(store) ||
      typeof store.store_id !== "number" ||
      !Number.isSafeInteger(store.store_id) ||
      store.store_id < 1
    )
      throw new SourceSubscriptionError("Invalid subscribed store list.", 503);
    return store.store_id;
  });
  if (!ids.length)
    throw new SourceSubscriptionError(
      "Select at least one source store first.",
      403,
    );
  // Expiry is evaluated by the source service against its database clock.
  return { storeIds: [...new Set(ids)] };
}
