import { useEffect, useRef, useState } from "react";
import type {
  ActionFunctionArgs,
  HeadersFunction,
  LoaderFunctionArgs,
} from "react-router";
import { data, useFetcher, useLoaderData } from "react-router";
import { boundary } from "@shopify/shopify-app-react-router/server";

import { authenticate } from "../shopify.server";
import {
  AccountError,
  loadAccount,
  saveAccount,
} from "../services/account.server";

export const loader = async ({ request }: LoaderFunctionArgs) => {
  const { session } = await authenticate.admin(request);

  return {
    shop: session.shop,
    account: await loadAccount(session.shop),
  };
};

export const action = async ({ request }: ActionFunctionArgs) => {
  const { session } = await authenticate.admin(request);
  const form = await request.formData();

  try {
    await saveAccount(session.shop, form);

    return data({
      ok: true,
      message: "Your account settings have been updated.",
    });
  } catch (error) {
    return data(
      {
        ok: false,
        message:
          error instanceof AccountError
            ? error.message
            : "Unable to save settings. Please try again.",
      },
      { status: error instanceof AccountError ? 400 : 500 },
    );
  }
};

export default function AccountAndPlan() {
  const { shop, account } = useLoaderData<typeof loader>();
  const fetcher = useFetcher<typeof action>();

  const [token, setToken] = useState("");
  const [settings, setSettings] = useState(account.settings);

  const feedbackRef = useRef<HTMLDivElement>(null);
  const busy = fetcher.state !== "idle";

  useEffect(() => {
    if (fetcher.state !== "idle" || !fetcher.data) {
      return;
    }

    if (fetcher.data.ok) {
      setToken("");
      setSettings(account.settings);
    }

    feedbackRef.current?.scrollIntoView({
      behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches
        ? "auto"
        : "smooth",
      block: "start",
    });
  }, [fetcher.state, fetcher.data, account.settings]);

  function updateSetting<K extends keyof typeof settings>(
    name: K,
    value: (typeof settings)[K],
  ) {
    setSettings((current) => ({ ...current, [name]: value }));
  }

  return (
    <s-page heading="Account & Plan">
      <form
        onSubmit={(event) => {
          event.preventDefault();

          if (busy) {
            return;
          }

          void fetcher.submit(
            {
              purchase_token: token,
              defaultMargin: settings.defaultMargin,
              minPrice: settings.minPrice,
              maxPrice: settings.maxPrice,
              skipUnmapped: String(settings.skipUnmapped),
            },
            { method: "post", action: "/app?index" },
          );
        }}
      >
        <s-stack direction="block" gap="base">
          {!busy && fetcher.data && (
            <div
              ref={feedbackRef}
              aria-live="polite"
              style={{ scrollMarginTop: "24px" }}
            >
              <s-banner
                tone={fetcher.data.ok ? "success" : "critical"}
                heading={
                  fetcher.data.ok ? "Settings saved" : "Settings not saved"
                }
              >
                <s-paragraph>{fetcher.data.message}</s-paragraph>
              </s-banner>
            </div>
          )}

          <s-section heading="Shopify connection">
            <s-paragraph>Connected store: {shop}</s-paragraph>
          </s-section>

          <s-section heading="Purchase token">
            <s-stack direction="block" gap="base">
              <s-password-field
                label="Purchase token"
                name="purchase_token"
                autocomplete="off"
                value={token}
                disabled={busy}
                placeholder={
                  account.hasToken
                    ? "A purchase token is already saved"
                    : "Enter your purchase token"
                }
                onInput={(event) => {
                  setToken(event.currentTarget.value);
                }}
              />

              <s-paragraph>
                {account.hasToken
                  ? "Leave this field blank to keep your saved token, or enter a replacement."
                  : "Enter the token you received after purchase."}
              </s-paragraph>
            </s-stack>
          </s-section>

          <s-section heading="Pricing">
            <s-stack direction="block" gap="base">
              <s-number-field
                label="Default Profit Margin"
                name="defaultMargin"
                min={0}
                step={1}
                value={settings.defaultMargin}
                disabled={busy}
                onInput={(event) =>
                  updateSetting("defaultMargin", event.currentTarget.value)
                }
              />

              <s-paragraph>
                A fixed amount added to the source product price. Category
                margins can be configured under Category Mapping.
              </s-paragraph>
            </s-stack>
          </s-section>

          <s-section heading="Import Filters">
            <s-stack direction="block" gap="base">
              <s-number-field
                label="Minimum source price"
                name="minPrice"
                min={0}
                step={1}
                value={settings.minPrice}
                disabled={busy}
                onInput={(event) =>
                  updateSetting("minPrice", event.currentTarget.value)
                }
              />

              <s-number-field
                label="Maximum source price"
                name="maxPrice"
                min={0}
                step={1}
                value={settings.maxPrice}
                disabled={busy}
                onInput={(event) =>
                  updateSetting("maxPrice", event.currentTarget.value)
                }
              />

              <s-paragraph>
                Products outside this range, before adding profit margin, will
                be skipped. Set a limit to 0 to disable it.
              </s-paragraph>

              <s-checkbox
                label="Skip products if category is not mapped"
                name="skipUnmapped"
                checked={settings.skipUnmapped}
                disabled={busy}
                onChange={(event) =>
                  updateSetting("skipUnmapped", event.currentTarget.checked)
                }
              />
            </s-stack>
          </s-section>

          <s-button
            type="submit"
            variant="primary"
            disabled={busy}
            loading={busy}
          >
            Save settings
          </s-button>
        </s-stack>
      </form>
    </s-page>
  );
}

export const headers: HeadersFunction = (headersArgs) =>
  boundary.headers(headersArgs);
