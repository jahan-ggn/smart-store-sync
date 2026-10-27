import type { ActionFunctionArgs } from "react-router";
import { authenticate } from "../shopify.server";
import { redactShopData } from "../services/privacy.server";

export const action = async ({ request }: ActionFunctionArgs) => {
  // Shopify's SDK checks the raw-body HMAC, including after uninstall when
  // there is no stored session. Invalid signatures receive HTTP 401.
  const { topic, shop, payload } = await authenticate.webhook(request);
  if (payload.shop_domain !== shop) {
    return new Response("Shop domain mismatch.", { status: 400 });
  }
  switch (topic) {
    case "CUSTOMERS_DATA_REQUEST":
    case "CUSTOMERS_REDACT":
      // This catalog importer does not collect Shopify customer/order data.
      // Do not persist or log the customer details in the webhook payload.
      return new Response(null, { status: 200 });
    case "SHOP_REDACT":
      await redactShopData(shop);
      return new Response(null, { status: 200 });
    default:
      return new Response("Unsupported privacy topic.", { status: 400 });
  }
};
