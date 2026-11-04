import type { CSSProperties } from "react";

export const meta = () => [
  { title: "Privacy Policy | Smart Store Sync" },
  {
    name: "description",
    content: "How Smart Store Sync collects, uses, and handles merchant data.",
  },
];

const sectionStyle: CSSProperties = { marginTop: "2rem" };

// Public route: privacy information must be readable before app installation.
export default function PrivacyPolicy() {
  return (
    <main
      style={{
        maxWidth: "820px",
        margin: "0 auto",
        padding: "48px 24px 72px",
        fontFamily: "Inter, system-ui, sans-serif",
        lineHeight: 1.75,
        color: "#17373b",
        overflowWrap: "anywhere",
      }}
    >
      <header>
        <p>Smart Store Sync</p>
        <h1>Privacy Policy</h1>
        <p>Last updated: November 4, 2026</p>
        <p>
          Smart Store Sync is operated from India. This policy explains how we
          handle information when merchants install or use our Shopify app,
          connect source catalogs, or contact us for support.
        </p>
      </header>

      <section style={sectionStyle}>
        <h2>1. Information we process</h2>
        <ul>
          <li>
            <strong>Shopify connection:</strong> your shop domain,
            authentication tokens, permissions, and session details. Session
            records may include staff identity information supplied by Shopify
            during authentication.
          </li>
          <li>
            <strong>Catalog data:</strong> products, variants, prices,
            inventory, images, collections, locations, and publishing details.
          </li>
          <li>
            <strong>App settings:</strong> your source access token,
            subscription status, selected stores, category mappings, margins,
            and import filters.
          </li>
          <li>
            <strong>Operational data:</strong> import files, job progress,
            timestamps, errors, and service logs.
          </li>
        </ul>
        <p>
          The app does not request Shopify customer or order access and does not
          import customer lists, order histories, or payment-card details. We do
          not install storefront advertising pixels or shopper-behavior
          tracking. Please do not include customer personal information in
          catalog files or support messages unless necessary for your request.
        </p>
      </section>

      <section style={sectionStyle}>
        <h2>2. How we use information</h2>
        <p>
          We use information to authenticate your store, verify source access,
          apply your settings, import and update products, synchronize inventory
          and images, assign collections, and publish eligible products. We also
          use operational information to retry deliveries, prevent duplicate
          imports, troubleshoot failures, secure the service, respond to support
          requests, and meet applicable legal obligations.
        </p>
        <p>
          Where applicable, our processing is based on providing the service you
          request, our legitimate interests in operating and securing it, legal
          obligations, or your consent when required.
        </p>
      </section>

      <section style={sectionStyle}>
        <h2>3. Service providers and sharing</h2>
        <p>
          We share information with Shopify and our source-catalog backend as
          needed to authenticate your store, verify source access, and
          synchronize products, inventory, and images.
        </p>
        <p>
          We use service providers for hosting, network services, and image
          storage and delivery. These providers process information needed to
          operate the service. We may also disclose information where legally
          required. We do not sell personal information or share it for targeted
          advertising.
        </p>
      </section>

      <section style={sectionStyle}>
        <h2>4. Storage, security, and international processing</h2>
        <p>
          App settings and authentication records are stored in our service
          database; retained catalog files are stored on our servers. Saved
          source access tokens are encrypted by the app. We use HTTPS for
          service connections and authenticate Shopify webhook requests. We take
          reasonable measures to protect your information against unauthorized
          access, loss, and misuse.
        </p>
        <p>
          We operate from India. Our hosting, network, and platform providers
          may process information in other countries, including outside your
          country of residence. Contact us for information about the processing
          locations and transfer arrangements relevant to your use of the
          service.
        </p>
      </section>

      <section style={sectionStyle}>
        <h2>5. Retention and deletion</h2>
        <p>
          We retain connection details and settings while you use the app.
          Completed, failed, and cancelled import records and their files become
          eligible for cleanup after seven days without an update. Cleanup runs
          when new imports are prepared, so deletion may occur later.
        </p>
        <p>
          When Shopify notifies us of an uninstall, the app cancels active
          imports and removes saved Shopify sessions. Shopify’s shop-deletion
          notification triggers removal of settings, source credentials,
          category mappings, and retained import data.
        </p>
        <p>
          Products and images already imported into Shopify remain in your store
          for you to manage.
        </p>
      </section>

      <section style={sectionStyle}>
        <h2>6. Your choices and rights</h2>
        <p>
          You can update your settings in the app and revoke Shopify access by
          uninstalling it. Depending on applicable law, you may request access,
          correction, deletion, a copy of your personal information, or
          restriction of or objection to processing. You may also withdraw
          consent where processing depends on it and complain to your local
          data-protection authority. Contact us using the email below; we may
          verify your identity and store ownership before acting on a request.
        </p>
        <p>
          Shopify customer-data webhooks are handled without retaining customer
          details because this app does not maintain Shopify customer records.
          Questions about a merchant&apos;s customers should also be directed to
          that merchant.
        </p>
      </section>

      <section style={sectionStyle}>
        <h2>7. Cookies and similar technologies</h2>
        <p>
          Smart Store Sync stores authentication and session information
          required to securely connect your store with Shopify. We do not add
          advertising cookies or track shoppers’ browsing activity. Shopify and
          our infrastructure providers may use their own cookies or similar
          technologies under their respective privacy policies.
        </p>
      </section>

      <section style={sectionStyle}>
        <h2>8. Changes and contact</h2>
        <p>
          We may update this policy as the service changes. Updates will appear
          here with a revised date; we will provide additional notice where
          required.
        </p>
        <p>
          For privacy questions or requests, contact us at{" "}
          <a href="mailto:info@kodyt.com">info@kodyt.com</a>. Please include
          your shop domain and the nature of your request, but never send access
          tokens or passwords by email.
        </p>
      </section>
    </main>
  );
}
