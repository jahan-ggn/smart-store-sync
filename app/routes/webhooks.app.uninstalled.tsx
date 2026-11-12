import type { ActionFunctionArgs } from "react-router";
import { authenticate } from "../shopify.server";
import {
  BillingUnavailable,
  queueSourceBillingRevocation,
} from "../services/billing.server";

export const action = async ({ request }: ActionFunctionArgs) => {
  const { shop, topic } = await authenticate.webhook(request);

  if (topic !== "APP_UNINSTALLED") {
    return new Response("Unexpected webhook topic", { status: 400 });
  }

  try {
    await queueSourceBillingRevocation(
      shop,
      request.headers.get("X-Shopify-Triggered-At") ?? "",
    );
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
