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
  CategoryMappingError,
  loadCategoryMapping,
  saveCategoryMapping,
} from "../services/categories.server";
import { StoresError } from "../services/stores.server";

function errorMessage(error: unknown) {
  return error instanceof CategoryMappingError || error instanceof StoresError
    ? error.message
    : "Unable to complete the request. Please try again.";
}

export const loader = async ({ request }: LoaderFunctionArgs) => {
  const { session, admin } = await authenticate.admin(request);

  try {
    return {
      page: await loadCategoryMapping(session.shop, admin),
      error: null,
    };
  } catch (error) {
    if (error instanceof Response) throw error;

    console.error(
      "Category Mapping loader failed:",
      error instanceof Error
        ? { name: error.name, message: error.message }
        : { type: typeof error },
    );

    return {
      page: null,
      error: errorMessage(error),
    };
  }
};

export const action = async ({ request }: ActionFunctionArgs) => {
  const { session, admin } = await authenticate.admin(request);
  const form = await request.formData();

  try {
    await saveCategoryMapping(session.shop, admin, form);

    return data({
      ok: true,
      message: "Your category mappings have been updated.",
    });
  } catch (error) {
    if (error instanceof Response) throw error;

    return data(
      {
        ok: false,
        message: errorMessage(error),
      },
      {
        status: error instanceof CategoryMappingError ? 400 : 500,
      },
    );
  }
};

type Page = Awaited<ReturnType<typeof loadCategoryMapping>>;
type Store = Page["stores"][number];

export default function CategoryMapping() {
  const { page, error } = useLoaderData<typeof loader>();
  const [selectedId, setSelectedId] = useState<number | null>(null);

  if (!page) {
    return (
      <s-page heading="Category Mapping">
        <s-banner tone="critical" heading="Unable to load category mappings">
          <s-paragraph>{error}</s-paragraph>

          <s-paragraph>
            Check Account &amp; Plan and Stores, then reload this page.
          </s-paragraph>
        </s-banner>
      </s-page>
    );
  }

  const store =
    page.stores.find((item) => item.id === selectedId) ?? page.stores[0];

  if (!store) {
    return (
      <s-page heading="Category Mapping">
        <s-section heading="Select your source stores first">
          <s-paragraph>
            Enable at least one source store before mapping its categories.
          </s-paragraph>

          <s-link href="/app/stores">Go to Stores</s-link>
        </s-section>
      </s-page>
    );
  }

  return (
    <MappingForm
      key={store.id}
      page={page}
      store={store}
      onStoreChange={setSelectedId}
    />
  );
}

function MappingForm({
  page,
  store,
  onStoreChange,
}: {
  page: Page;
  store: Store;
  onStoreChange: (id: number) => void;
}) {
  const fetcher = useFetcher<typeof action>();
  const [rows, setRows] = useState(store.rows);
  const feedbackRef = useRef<HTMLDivElement>(null);

  const busy = fetcher.state !== "idle";
  const dirty = JSON.stringify(rows) !== JSON.stringify(store.rows);

  const collectionIds = new Set(
    page.collections.map((collection) => collection.id),
  );

  useEffect(() => {
    if (fetcher.state === "idle" && fetcher.data) {
      if (fetcher.data.ok) {
        setRows(store.rows);
      }

      feedbackRef.current?.scrollIntoView({
        behavior: "smooth",
        block: "start",
      });
    }
  }, [fetcher.state, fetcher.data, store.rows]);

  function updateRow(
    categoryId: number,
    patch: Partial<
      Pick<Store["rows"][number], "collectionId" | "profitMargin">
    >,
  ) {
    setRows((current) =>
      current.map((row) =>
        row.categoryId === categoryId ? { ...row, ...patch } : row,
      ),
    );
  }

  return (
    <s-page heading="Category Mapping">
      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (busy) return;

          void fetcher.submit(
            {
              store_id: String(store.id),
              mappings: JSON.stringify(rows),
            },
            {
              method: "post",
              action: "/app/mappings",
            },
          );
        }}
      >
        <s-stack direction="block" gap="base">
          <div ref={feedbackRef}>
            {!busy && fetcher.data && (
              <s-banner
                tone={fetcher.data.ok ? "success" : "critical"}
                heading={
                  fetcher.data.ok ? "Mappings saved" : "Mappings not saved"
                }
              >
                <s-paragraph>{fetcher.data.message}</s-paragraph>
              </s-banner>
            )}
          </div>

          <s-section heading="Source store">
            <s-stack direction="block" gap="base">
              <s-select
                label="Source store"
                value={String(store.id)}
                disabled={busy || dirty}
                onChange={(event) =>
                  onStoreChange(Number(event.currentTarget.value))
                }
              >
                {page.stores.map((item) => (
                  <s-option key={item.id} value={String(item.id)}>
                    {item.name}
                  </s-option>
                ))}
              </s-select>

              <s-paragraph>
                Only stores enabled on the Stores page are listed.
              </s-paragraph>

              {dirty && (
                <s-paragraph>
                  Save or discard your changes before switching stores.
                </s-paragraph>
              )}
            </s-stack>
          </s-section>

          {page.collections.length === 0 && (
            <s-banner tone="info" heading="No Shopify collections found">
              <s-paragraph>
                Create a collection under Products → Collections, then reload
                this page.
              </s-paragraph>
            </s-banner>
          )}

          <s-section heading="Category mappings">
            <s-stack direction="block" gap="base">
              <s-paragraph>
                Choose a Shopify collection for each source category. Category
                margins are fixed amounts, not percentages. During import, the
                highest applicable margin, including the default profit margin,
                will be used.
              </s-paragraph>

              {rows.length === 0 ? (
                <s-paragraph>
                  No source categories are available for this store.
                </s-paragraph>
              ) : (
                <s-table>
                  <s-table-header-row>
                    <s-table-header listSlot="primary">
                      Source category
                    </s-table-header>

                    <s-table-header listSlot="labeled">
                      Shopify collection
                    </s-table-header>

                    <s-table-header listSlot="labeled">
                      Profit margin
                    </s-table-header>
                  </s-table-header-row>

                  <s-table-body>
                    {rows.map((row) => (
                      <s-table-row key={row.categoryId}>
                        <s-table-cell>{row.categoryName}</s-table-cell>

                        <s-table-cell>
                          <s-select
                            label={`Collection for ${row.categoryName}`}
                            labelAccessibilityVisibility="exclusive"
                            value={row.collectionId}
                            disabled={busy}
                            onChange={(event) =>
                              updateRow(row.categoryId, {
                                collectionId: event.currentTarget.value,
                              })
                            }
                          >
                            <s-option value="">Not mapped</s-option>

                            {row.collectionId &&
                              !collectionIds.has(row.collectionId) && (
                                <s-option value={row.collectionId}>
                                  Collection unavailable — select another
                                </s-option>
                              )}

                            {page.collections.map((collection) => (
                              <s-option
                                key={collection.id}
                                value={collection.id}
                              >
                                {collection.title}
                              </s-option>
                            ))}
                          </s-select>
                        </s-table-cell>

                        <s-table-cell>
                          <s-number-field
                            label={`Profit margin for ${row.categoryName}`}
                            labelAccessibilityVisibility="exclusive"
                            min={0}
                            max={9999999999}
                            step={1}
                            inputMode="numeric"
                            required
                            value={row.profitMargin}
                            disabled={busy}
                            onInput={(event) =>
                              updateRow(row.categoryId, {
                                profitMargin: event.currentTarget.value,
                              })
                            }
                          />
                        </s-table-cell>
                      </s-table-row>
                    ))}
                  </s-table-body>
                </s-table>
              )}
            </s-stack>
          </s-section>

          {rows.length > 0 && (
            <s-stack direction="inline" gap="base">
              <s-button
                type="submit"
                variant="primary"
                disabled={busy}
                loading={busy}
              >
                Save mappings
              </s-button>

              <s-button
                type="button"
                disabled={busy || !dirty}
                onClick={() => setRows(store.rows)}
              >
                Discard changes
              </s-button>
            </s-stack>
          )}
        </s-stack>
      </form>
    </s-page>
  );
}

export const headers: HeadersFunction = (headersArgs) =>
  boundary.headers(headersArgs);
