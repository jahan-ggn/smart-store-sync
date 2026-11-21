import { authenticate } from "../shopify.server";
import { syncShopifyBilling } from "./billing.server";

export async function requireApprovedPlan(request: Request) {
  const context = await authenticate.admin(request);

  try {
    const billing = await syncShopifyBilling(
      context.session.shop,
      context.admin,
    );

    if (billing.status === "trial" || billing.status === "active") {
      return context;
    }
  } catch (error) {
    if (error instanceof Response) throw error;
  }

  throw context.redirect("/app");
}
