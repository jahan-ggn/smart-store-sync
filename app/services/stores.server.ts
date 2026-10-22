import process from "node:process";

import { getSavedPurchaseToken } from "./account.server";

export class StoresError extends Error {}

type SourceStore = {
  id: number;
  name: string;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isId(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

async function sourceRequest(
  path: string,
  body?: Record<string, unknown>,
): Promise<unknown> {
  const baseUrl = process.env.SOURCE_API_BASE_URL?.replace(/\/+$/, "");

  if (!baseUrl) {
    throw new StoresError("The source service is not configured.");
  }

  let response: Response;

  try {
    response = await fetch(`${baseUrl}${path}`, {
      method: body ? "POST" : "GET",
      headers: body ? { "Content-Type": "application/json" } : undefined,
      body: body ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(15_000),
      cache: "no-store",
      redirect: "error",
    });
  } catch {
    throw new StoresError(
      "Unable to reach the source service. If you were saving, reload Stores before retrying.",
    );
  }

  if (response.status === 403) {
    throw new StoresError(
      "Your subscription or store selection was rejected. Check the purchase token in Account & Plan.",
    );
  }

  if (!response.ok) {
    throw new StoresError(
      "The source service could not complete the request. Please try again.",
    );
  }

  try {
    return await response.json();
  } catch {
    throw new StoresError(
      "The source service returned an invalid response. Reload Stores before retrying.",
    );
  }
}

async function subscriptionCredentials(shop: string) {
  const token = await getSavedPurchaseToken(shop);

  if (!token) {
    throw new StoresError("Save your purchase token in Account & Plan first.");
  }

  return {
    token,
    buyer_domain: `https://${shop}`,
  };
}

export async function loadStores(shop: string) {
  const credentials = await subscriptionCredentials(shop);

  const status = await sourceRequest("/subscriptions/status", credentials);

  if (
    !isRecord(status) ||
    status.success !== true ||
    !isRecord(status.data) ||
    !Array.isArray(status.data.selected_stores)
  ) {
    throw new StoresError("Unexpected subscription response.");
  }

  const selectedIds = status.data.selected_stores.map((store) => {
    if (!isRecord(store) || !isId(store.store_id)) {
      throw new StoresError("Invalid selected-store response.");
    }

    return store.store_id;
  });

  const catalog = await sourceRequest("/stores");

  if (!Array.isArray(catalog)) {
    throw new StoresError("Unexpected store-list response.");
  }

  const stores: SourceStore[] = catalog.map((store) => {
    if (
      !isRecord(store) ||
      !isId(store.store_id) ||
      typeof store.store_name !== "string"
    ) {
      throw new StoresError("Invalid store-list response.");
    }

    return {
      id: store.store_id,
      name: store.store_name,
    };
  });

  return { stores, selectedIds };
}

export async function saveStores(shop: string, form: FormData) {
  const ids = form.getAll("store_ids").map((value) => {
    if (typeof value !== "string" || !/^\d+$/.test(value)) {
      throw new StoresError("Invalid store selection.");
    }

    const id = Number(value);

    if (!isId(id)) {
      throw new StoresError("Invalid store selection.");
    }

    return id;
  });

  const storeIds = [...new Set(ids)];

  if (storeIds.length === 0) {
    throw new StoresError("Select at least one store.");
  }

  const credentials = await subscriptionCredentials(shop);

  const result = await sourceRequest("/subscriptions/permissions", {
    ...credentials,
    store_ids: storeIds,
  });

  if (
    !isRecord(result) ||
    result.success !== true ||
    !isRecord(result.data) ||
    result.data.total_stores !== storeIds.length
  ) {
    throw new StoresError(
      "The backend did not confirm the save. Reload Stores before retrying.",
    );
  }
}
