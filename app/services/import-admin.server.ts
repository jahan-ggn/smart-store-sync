import type { unauthenticated } from "../shopify.server";
import { checkImportShop } from "./import-jobs.server";

type LoadContext = typeof unauthenticated.admin;
type Context = Awaited<ReturnType<LoadContext>>;
type Admin = Context["admin"];

async function loadOfflineContext(shop: string) {
  const { unauthenticated } = await import("../shopify.server");
  return unauthenticated.admin(shop);
}

function isUnauthorized(error: unknown) {
  if (error instanceof Response) return error.status === 401;
  if (typeof error !== "object" || error === null) return false;
  const failure = error as {
    status?: number;
    response?: { code?: number; status?: number };
  };
  return (
    failure.status === 401 ||
    failure.response?.code === 401 ||
    failure.response?.status === 401
  );
}

function authenticationError() {
  return new Error(
    "Shopify authentication is no longer valid. Open this app in Shopify Admin to reconnect, then resume the same import job. Saved progress is retained.",
  );
}

// One shared client per import process. The SDK loads current credentials and
// refreshes tokens within five minutes of expiry, persisting the rotated pair.
export function createImportAdmin(
  shop: string,
  loadContext: LoadContext = loadOfflineContext,
): Admin {
  checkImportShop(shop);
  let loading: Promise<Context> | undefined;
  async function currentContext() {
    // Coalesce concurrent loads/refreshes rather than rotate a refresh token
    // independently for each of the importer's parallel API requests.
    if (!loading) {
      loading = Promise.resolve()
        .then(() => loadContext(shop))
        .then((context) => {
          const { session } = context;
          if (
            session.shop !== shop ||
            session.isOnline ||
            !session.accessToken ||
            session.isExpired()
          )
            throw authenticationError();
          return context;
        });
    }
    const pending = loading;
    try {
      return await pending;
    } finally {
      if (loading === pending) loading = undefined;
    }
  }

  const graphql: Admin["graphql"] = async (query, options) => {
    options?.signal?.throwIfAborted();
    const context = await currentContext();
    options?.signal?.throwIfAborted();
    try {
      const response = await context.admin.graphql(query, options);
      if (response.status === 401) throw response;
      return response;
    } catch (error) {
      if (!isUnauthorized(error)) throw error;
      options?.signal?.throwIfAborted();
      // Another process (for example the app UI) may have rotated credentials
      // while this request was in flight. Reload once and only retry a 401
      // when a different token is available; never replay ambiguous failures.
      const updated = await currentContext();
      if (updated.session.accessToken === context.session.accessToken)
        throw authenticationError();
      options?.signal?.throwIfAborted();
      try {
        const response = await updated.admin.graphql(query, options);
        if (response.status === 401) throw authenticationError();
        return response;
      } catch (retryError) {
        if (isUnauthorized(retryError)) throw authenticationError();
        throw retryError;
      }
    }
  };
  return { graphql };
}
