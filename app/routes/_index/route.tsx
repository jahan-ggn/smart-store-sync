import type { LoaderFunctionArgs } from "react-router";
import { redirect } from "react-router";

import styles from "./styles.module.css";

export const loader = async ({ request }: LoaderFunctionArgs) => {
  const url = new URL(request.url);

  if (url.searchParams.get("shop")) {
    throw redirect(`/app?${url.searchParams.toString()}`);
  }

  return null;
};

export default function App() {
  return (
    <div className={styles.index}>
      <div className={styles.shell}>
        <header className={styles.header}>
          <a
            className={styles.brand}
            href="/"
            aria-label="Smart Store Sync home"
          >
            <span className={styles.brandIcon} aria-hidden="true">
              <svg
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.7"
              >
                <path d="M4 10v10h16V10M3 10l2-6h14l2 6M3 10c0 3 4 3 4 0 0 3 5 3 5 0 0 3 5 3 5 0 0 3 4 3 4 0M9 20v-6h6v6" />
              </svg>
            </span>
            Smart Store Sync
          </a>
          <a className={styles.supportLink} href="mailto:info@kodyt.com">
            Contact support <span aria-hidden="true">↗</span>
          </a>
        </header>

        <main>
          <section className={styles.hero} aria-labelledby="hero-title">
            <div className={styles.intro}>
              <p className={styles.eyebrow}>
                <span aria-hidden="true" /> CATALOG SYNC, SIMPLIFIED
              </p>
              <h1 id="hero-title" className={styles.heading}>
                Your Cartpe catalog.
                <br />
                <span>In sync with Shopify.</span>
              </h1>
              <p className={styles.description}>
                Bring your source products into your store and keep prices,
                inventory, and images updated. Your catalog, with your rules.
              </p>
              <div
                className={styles.flow}
                aria-label="Sync products from Cartpe to Shopify"
              >
                <span className={styles.flowStore}>
                  <span aria-hidden="true">C</span> Cartpe
                </span>
                <span className={styles.flowArrow} aria-hidden="true">
                  ⇄
                </span>
                <span className={styles.flowStore}>
                  <span aria-hidden="true">S</span> Shopify
                </span>
              </div>
              <p className={styles.flowNote}>
                Connected catalogs. Less manual work.
              </p>
            </div>

            <div className={styles.connection}>
              <span className={styles.cardEyebrow}>YOUR WORKSPACE</span>
              <h2>Open from Shopify</h2>
              <p className={styles.cardDescription}>
                Already installed? Access Smart Store Sync directly from your
                Shopify admin.
              </p>
              <div className={styles.form}>
                <ol className={styles.help}>
                  <li>Sign in to Shopify and choose your store.</li>
                  <li>Open Apps in your Shopify admin.</li>
                  <li>Select Smart Store Sync.</li>
                </ol>
                <a
                  className={styles.button}
                  style={{ textDecoration: "none" }}
                  href="https://admin.shopify.com/"
                >
                  Go to Shopify admin <span aria-hidden="true">→</span>
                </a>
              </div>
              <p className={styles.cardFootnote}>
                Need help getting started?{" "}
                <a href="mailto:info@kodyt.com">Contact our support team.</a>
              </p>
            </div>
          </section>

          <section className={styles.features} aria-labelledby="features-title">
            <div className={styles.sectionHeading}>
              <h2 id="features-title">
                One connection. A coordinated catalog.
              </h2>
              <p>
                Choose your sources. Set your rules. Let the updates follow.
              </p>
            </div>
            <ul className={styles.list}>
              <li>
                <span className={styles.featureNumber} aria-hidden="true">
                  01
                </span>
                <h3>Choose your source stores</h3>
                <p>
                  Connect your source subscription and select the Cartpe stores
                  you want to import from.
                </p>
              </li>
              <li>
                <span className={styles.featureNumber} aria-hidden="true">
                  02
                </span>
                <h3>Make the catalog yours</h3>
                <p>
                  Map categories to Shopify collections, apply profit margins,
                  and filter which products are imported.
                </p>
              </li>
              <li>
                <span className={styles.featureNumber} aria-hidden="true">
                  03
                </span>
                <h3>Keep it up to date</h3>
                <p>
                  Automatically sync product details, inventory, and images.
                  Products without a ready main image stay as drafts.
                </p>
              </li>
            </ul>
          </section>
        </main>

        <footer className={styles.footer}>
          <span>
            Smart Store Sync{" "}
            <span className={styles.footerDot} aria-hidden="true">
              ·
            </span>{" "}
            Catalogs, connected.
          </span>
          <nav aria-label="Footer">
            <a href="/privacy">Privacy policy</a>
            <a href="mailto:info@kodyt.com">Support</a>
          </nav>
        </footer>
      </div>
    </div>
  );
}
