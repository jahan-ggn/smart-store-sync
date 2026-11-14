import { useEffect, useRef, useState } from "react";
import type {
  ActionFunctionArgs,
  HeadersFunction,
  LoaderFunctionArgs,
} from "react-router";
import { data, useFetcher, useLoaderData, useRevalidator } from "react-router";
import { boundary } from "@shopify/shopify-app-react-router/server";

import { authenticate } from "../shopify.server";
import {
  AccountError,
  ensureSourceAccount,
  loadAccount,
  saveAccount,
  SourceProvisioningError,
} from "../services/account.server";
import process from "node:process";
import {
  syncShopifyBilling,
  type BillingState,
} from "../services/billing.server";
import { requireApprovedPlan } from "../services/require-plan.server";

export const loader = async ({ request }: LoaderFunctionArgs) => {
  const { session, admin } = await authenticate.admin(request);

  let connectionError: string | null = null;
  let billing: BillingState | null = null;
  let billingError: string | null = null;

  try {
    await ensureSourceAccount(session.shop);
  } catch (error) {
    connectionError =
      error instanceof SourceProvisioningError
        ? error.message
        : "Unable to connect your source account. Please try again.";
  }

  if (!connectionError) {
    try {
      billing = await syncShopifyBilling(session.shop, admin);
    } catch {
      billingError = "Unable to refresh your billing status. Please try again.";
    }
  }

  const handle = process.env.SHOPIFY_APP_HANDLE ?? "";
  const planUrl = /^[a-z0-9][a-z0-9-]*$/.test(handle)
    ? `https://admin.shopify.com/store/${session.shop.replace(/\.myshopify\.com$/, "")}/charges/${handle}/pricing_plans`
    : null;

  return {
    shop: session.shop,
    account: await loadAccount(session.shop),
    connectionError,
    billing,
    billingError,
    planUrl,
  };
};

export const action = async ({ request }: ActionFunctionArgs) => {
  const { session } = await requireApprovedPlan(request);
  const form = await request.formData();

  try {
    await ensureSourceAccount(session.shop);
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
          error instanceof AccountError ||
          error instanceof SourceProvisioningError
            ? error.message
            : "Unable to save settings. Please try again.",
      },
      {
        status:
          error instanceof AccountError
            ? 400
            : error instanceof SourceProvisioningError
              ? 503
              : 500,
      },
    );
  }
};

export default function AccountAndPlan() {
  const { shop, account, connectionError, billing, billingError, planUrl } =
    useLoaderData<typeof loader>();
  const revalidator = useRevalidator();
  const fetcher = useFetcher<typeof action>();

  const [settings, setSettings] = useState(account.settings);

  const feedbackRef = useRef<HTMLDivElement>(null);
  const busy = fetcher.state !== "idle";

  useEffect(() => {
    if (fetcher.state !== "idle" || !fetcher.data) {
      return;
    }

    if (fetcher.data.ok) {
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

  if (billing?.status !== "trial" && billing?.status !== "active") {
    return (
      <PlanWelcome
        shop={shop}
        connectionError={connectionError}
        billingError={billingError}
        canChoosePlan={billing?.status === "inactive"}
        planUrl={planUrl}
      />
    );
  }

  return (
    <s-page heading="Account & Plan">
      <form
        onSubmit={(event) => {
          event.preventDefault();

          if (busy || connectionError || !account.hasToken) {
            return;
          }

          void fetcher.submit(
            {
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

          <s-section heading="Shopify plan">
            <s-stack direction="block" gap="base">
              <s-paragraph>
                Standard: $25 USD per month, with a 3-day trial for eligible
                stores.
              </s-paragraph>

              {billingError ? (
                <s-banner tone="warning" heading="Billing status unavailable">
                  <s-paragraph>{billingError}</s-paragraph>
                </s-banner>
              ) : billing ? (
                <>
                  <s-paragraph>
                    {billing.status === "trial"
                      ? "Trial approved"
                      : billing.status === "active"
                        ? "Subscription active"
                        : "No approved subscription"}
                  </s-paragraph>

                  {billing.expiresAt && (
                    <s-paragraph>
                      Approved until:{" "}
                      {new Date(billing.expiresAt).toUTCString()}
                    </s-paragraph>
                  )}

                  {billing.cancelAtEndOfCycle && (
                    <s-paragraph>
                      Cancellation is scheduled at the end of the approved
                      period.
                    </s-paragraph>
                  )}
                </>
              ) : (
                <s-paragraph>
                  Connect your source account to check billing.
                </s-paragraph>
              )}

              {planUrl ? (
                <s-button href={planUrl} target="_top" variant="primary">
                  Manage Shopify plan
                </s-button>
              ) : (
                <s-paragraph>
                  Plan management is temporarily unavailable.
                </s-paragraph>
              )}

              <s-button
                type="button"
                disabled={busy || revalidator.state !== "idle"}
                loading={revalidator.state !== "idle"}
                onClick={() => void revalidator.revalidate()}
              >
                Refresh billing status
              </s-button>
            </s-stack>
          </s-section>

          {connectionError ? (
            <s-banner tone="critical" heading="Source account unavailable">
              <s-paragraph>{connectionError}</s-paragraph>

              <s-button
                type="button"
                disabled={busy || revalidator.state !== "idle"}
                loading={revalidator.state !== "idle"}
                onClick={() => void revalidator.revalidate()}
              >
                Retry connection
              </s-button>
            </s-banner>
          ) : (
            <s-section heading="Set up catalog sync">
              <s-stack direction="block" gap="base">
                <s-paragraph>
                  Your plan is approved. Choose your source stores, map their
                  categories to Shopify collections, then set your pricing and
                  import filters below.
                </s-paragraph>

                <s-button href="/app/stores" variant="primary">
                  Choose source stores
                </s-button>

                <s-link href="/app/mappings">Map categories</s-link>
              </s-stack>
            </s-section>
          )}

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
            disabled={busy || Boolean(connectionError) || !account.hasToken}
            loading={busy}
          >
            Save settings
          </s-button>
        </s-stack>
      </form>
    </s-page>
  );
}

export function PlanWelcome({
  shop,
  connectionError,
  billingError,
  canChoosePlan,
  planUrl,
}: {
  shop: string;
  connectionError: string | null;
  billingError: string | null;
  canChoosePlan: boolean;
  planUrl: string | null;
}) {
  const error = connectionError || billingError;
  return (
    <s-page heading="Welcome to Smart Store Sync">
      <s-stack direction="block" gap="base">
        <s-section heading="Connect your catalog to Shopify">
          <s-paragraph>
            Import products from your selected source stores, apply your pricing
            rules, and keep your catalog in sync.
          </s-paragraph>
          <s-paragraph>Connected store: {shop}</s-paragraph>
        </s-section>

        {error ? (
          <s-banner
            tone="warning"
            heading={
              connectionError
                ? "Unable to finish connecting your account"
                : "Billing status unavailable"
            }
          >
            <s-paragraph>{error}</s-paragraph>
          </s-banner>
        ) : canChoosePlan ? (
          <s-section heading="Choose your plan to get started">
            <s-stack direction="block" gap="base">
              <s-heading>Standard — $25 USD per month</s-heading>
              <s-paragraph>
                Includes a 3-day trial for eligible stores. Shopify confirms
                trial eligibility and subscription terms before you approve.
              </s-paragraph>
              <s-paragraph>
                Approve your plan on Shopify to enable imports. After approval,
                return here to choose source stores and map your categories.
              </s-paragraph>
              {planUrl ? (
                <s-button href={planUrl} target="_top" variant="primary">
                  Activate plan
                </s-button>
              ) : (
                <s-banner tone="warning" heading="Plan selection unavailable">
                  <s-paragraph>
                    Unable to open Shopify plan selection. Please try again
                    shortly.
                  </s-paragraph>
                </s-banner>
              )}
            </s-stack>
          </s-section>
        ) : (
          <s-banner tone="warning" heading="Billing status unavailable">
            <s-paragraph>
              Check your billing status before continuing setup.
            </s-paragraph>
          </s-banner>
        )}
      </s-stack>
    </s-page>
  );
}

export const headers: HeadersFunction = (headersArgs) =>
  boundary.headers(headersArgs);
