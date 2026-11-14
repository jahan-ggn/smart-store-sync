import type { HeadersFunction, LoaderFunctionArgs } from "react-router";
import {
  Outlet,
  useLoaderData,
  useRouteError,
  useRouteLoaderData,
} from "react-router";
import type { loader as accountLoader } from "./app._index";
import { boundary } from "@shopify/shopify-app-react-router/server";
import { AppProvider } from "@shopify/shopify-app-react-router/react";

import { authenticate } from "../shopify.server";

export const loader = async ({ request }: LoaderFunctionArgs) => {
  await authenticate.admin(request);

  // eslint-disable-next-line no-undef
  return { apiKey: process.env.SHOPIFY_API_KEY || "" };
};

export default function App() {
  const { apiKey } = useLoaderData<typeof loader>();
  const account = useRouteLoaderData<typeof accountLoader>("routes/app._index");

  const showSetupLinks =
    !account ||
    account.billing?.status === "trial" ||
    account.billing?.status === "active";

  return (
    <AppProvider apiKey={apiKey}>
      <s-app-nav>
        <s-link href="/app">
          {showSetupLinks ? "Account & Plan" : "Welcome"}
        </s-link>

        {showSetupLinks && (
          <>
            <s-link href="/app/stores">Stores</s-link>
            <s-link href="/app/mappings">Category Mapping</s-link>
          </>
        )}
      </s-app-nav>
      <Outlet />
    </AppProvider>
  );
}

// Shopify needs React Router to catch some thrown responses, so that their headers are included in the response.
export function ErrorBoundary() {
  return boundary.error(useRouteError());
}

export const headers: HeadersFunction = (headersArgs) => {
  return boundary.headers(headersArgs);
};
