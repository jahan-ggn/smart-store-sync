import type { ActionFunctionArgs } from "react-router";
import { authenticate } from "../shopify.server";
import { stopShopImports } from "../services/privacy.server";

export const action = async ({ request }: ActionFunctionArgs) => {
  const { shop, topic } = await authenticate.webhook(request);

  console.log(`Received ${topic} webhook for ${shop}`);

  await stopShopImports(shop);

  return new Response();
};
