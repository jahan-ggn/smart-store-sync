import type { ActionFunctionArgs } from "react-router";
import { authenticate } from "../shopify.server";
import { stopShopImports } from "../services/privacy.server";
import {
  BillingUnavailable,
  revokeSourceBilling,
} from "../services/billing.server";

export const action = async ({ request }: ActionFunctionArgs) => {
  const { shop, topic } = await authenticate.webhook(request);

  if (topic !== "APP_UNINSTALLED") {
    return new Response("Unexpected webhook topic", { status: 400 });
  }

  try {
    const current = await revokeSourceBilling(
      shop,
      request.headers.get("X-Shopify-Triggered-At") ?? "",
    );

    if (current) {
      await stopShopImports(shop);
    }
  } catch (error) {
    if (!(error instanceof BillingUnavailable)) throw error;

    return new Response("Unable to complete uninstall. Please retry.", {
      status: 503,
      headers: {
        "Cache-Control": "no-store",
        "Retry-After": "5",
      },
    });
  }

  return new Response();
};
