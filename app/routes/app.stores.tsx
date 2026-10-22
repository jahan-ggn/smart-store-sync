import { useEffect, useRef, useState } from "react";
import type {
  ActionFunctionArgs,
  HeadersFunction,
  LoaderFunctionArgs,
} from "react-router";
import { data, useFetcher, useLoaderData } from "react-router";
import { boundary } from "@shopify/shopify-app-react-router/server";

import { authenticate } from "../shopify.server";
import { StoresError, loadStores, saveStores } from "../services/stores.server";

export const loader = async ({ request }: LoaderFunctionArgs) => {
  const { session } = await authenticate.admin(request);

  try {
    return {
      ...(await loadStores(session.shop)),
      error: null,
    };
  } catch (error) {
    return {
      stores: [],
      selectedIds: [],
      error:
        error instanceof StoresError
          ? error.message
          : "Unable to load stores. Please try again.",
    };
  }
};

export const action = async ({ request }: ActionFunctionArgs) => {
  const { session } = await authenticate.admin(request);
  const form = await request.formData();

  try {
    await saveStores(session.shop, form);

    return data({
      ok: true,
      message: "Your store selection has been saved.",
    });
  } catch (error) {
    return data(
      {
        ok: false,
        message:
          error instanceof StoresError
            ? error.message
            : "Unable to save stores. Please try again.",
      },
      { status: error instanceof StoresError ? 400 : 500 },
    );
  }
};

export default function Stores() {
  const { stores, selectedIds, error } = useLoaderData<typeof loader>();
  const fetcher = useFetcher<typeof action>();

  const [selected, setSelected] = useState<number[]>(selectedIds);
  const feedbackRef = useRef<HTMLDivElement>(null);
  const busy = fetcher.state !== "idle";

  useEffect(() => {
    setSelected(selectedIds);
  }, [selectedIds]);

  useEffect(() => {
    if (fetcher.state === "idle" && fetcher.data) {
      feedbackRef.current?.scrollIntoView({
        behavior: "auto",
        block: "start",
      });
    }
  }, [fetcher.state, fetcher.data]);

  function toggleStore(id: number, checked: boolean) {
    setSelected((current) =>
      checked
        ? [...new Set([...current, id])]
        : current.filter((value) => value !== id),
    );
  }

  return (
    <s-page heading="Stores">
      <s-stack direction="block" gap="base">
        {!busy && fetcher.data && (
          <div
            ref={feedbackRef}
            aria-live="polite"
            style={{ scrollMarginTop: "24px" }}
          >
            <s-banner
              tone={fetcher.data.ok ? "success" : "critical"}
              heading={fetcher.data.ok ? "Stores saved" : "Stores not saved"}
            >
              <s-paragraph>{fetcher.data.message}</s-paragraph>
            </s-banner>
          </div>
        )}

        {error ? (
          <s-banner tone="warning" heading="Stores unavailable">
            <s-paragraph>{error}</s-paragraph>
            <s-link href="/app">Open Account &amp; Plan</s-link>
          </s-banner>
        ) : (
          <s-section heading="Source stores">
            <s-stack direction="block" gap="base">
              <s-paragraph>
                Select the source stores you want to import from. Select at
                least one store before saving.
              </s-paragraph>

              {stores.length === 0 ? (
                <s-paragraph>No source stores are available.</s-paragraph>
              ) : (
                <s-table>
                  <s-table-header-row>
                    <s-table-header listSlot="primary">Store</s-table-header>
                    <s-table-header listSlot="secondary">
                      Store ID
                    </s-table-header>
                  </s-table-header-row>

                  <s-table-body>
                    {stores.map((store) => (
                      <s-table-row key={store.id}>
                        <s-table-cell>
                          <s-checkbox
                            label={store.name}
                            checked={selected.includes(store.id)}
                            disabled={busy}
                            onChange={(event) =>
                              toggleStore(store.id, event.currentTarget.checked)
                            }
                          />
                        </s-table-cell>

                        <s-table-cell>{store.id}</s-table-cell>
                      </s-table-row>
                    ))}
                  </s-table-body>
                </s-table>
              )}

              <s-paragraph>Selected stores: {selected.length}</s-paragraph>

              <s-button
                variant="primary"
                disabled={busy || stores.length === 0 || selected.length === 0}
                loading={busy}
                onClick={() => {
                  if (busy) return;

                  const form = new FormData();

                  selected.forEach((id) =>
                    form.append("store_ids", String(id)),
                  );

                  void fetcher.submit(form, {
                    method: "post",
                    action: "/app/stores",
                  });
                }}
              >
                Save Stores
              </s-button>
            </s-stack>
          </s-section>
        )}
      </s-stack>
    </s-page>
  );
}

export const headers: HeadersFunction = (headersArgs) =>
  boundary.headers(headersArgs);
