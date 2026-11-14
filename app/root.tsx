import {
  Links,
  Meta,
  Outlet,
  Scripts,
  ScrollRestoration,
  useNavigation,
} from "react-router";

export const meta = () => [
  { title: "Smart Store Sync" },
  {
    name: "description",
    content:
      "Keep your Shopify catalog in sync with your subscribed source stores.",
  },
];

export default function App() {
  const navigation = useNavigation();
  const loading = navigation.state !== "idle";
  return (
    <html lang="en">
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width,initial-scale=1" />
        <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
        <link rel="preconnect" href="https://cdn.shopify.com/" />
        <link
          rel="stylesheet"
          href="https://cdn.shopify.com/static/fonts/inter/v4/styles.css"
        />
        <Meta />
        <Links />
      </head>
      <body>
        {loading && (
          <div
            role="status"
            aria-live="polite"
            style={{
              position: "fixed",
              inset: 0,
              zIndex: 9999,
              display: "grid",
              placeItems: "center",
              background: "rgba(255, 255, 255, 0.9)",
              fontFamily: "Inter, sans-serif",
            }}
          >
            <div style={{ textAlign: "center" }}>
              <progress aria-label="Loading" />
              <p>Loading, please wait…</p>
            </div>
          </div>
        )}
        <Outlet />
        <ScrollRestoration />
        <Scripts />
      </body>
    </html>
  );
}
