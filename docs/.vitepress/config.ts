import { defineConfig } from "vitepress";
import { withMermaid } from "vitepress-plugin-mermaid";

const SITE_DESCRIPTION =
  "Build reliable message-driven applications with end-to-end type safety, automatic schema validation, and AsyncAPI generation for AMQP/RabbitMQ in TypeScript";

// Versioned deploys (deploy-docs.yml): while a prerelease is in progress the
// workflow builds main's docs a second time as the "beta" site, overriding the
// base to /amqp-contract/beta/ via DOCS_BASE; the stable site (built from the
// latest stable tag) stays at the root. Absent env — local dev, plain builds —
// keeps today's root base.
// Normalized to a guaranteed leading + trailing slash — a DOCS_BASE typed
// without one in CI must not corrupt asset routing or canonical URLs.
const RAW_BASE = process.env.DOCS_BASE ?? "/amqp-contract/";
const BASE = `/${RAW_BASE.replace(/^\/+|\/+$/g, "")}/`;
const ORIGIN = "https://btravstack.github.io";
const SITE_URL = `${ORIGIN}${BASE}`;

// DOCS_VERSIONS — the marker the deploy workflow probes for to detect native
// version-picker support (legacy tags without it get the dropdown injected at
// build time instead). JSON: { current: string, items: { text, link }[] } —
// rendered as a dropdown at the end of the nav, linking every deployed version
// with absolute URLs (cross-version links must not inherit this build's base).
// Absent env → no dropdown (single-version site, local dev).
type VersionMenu = {
  current: string;
  items: { text: string; link: string; target?: string }[];
};

// The value is machine-generated (deploy-docs.yml stringifies it into
// GITHUB_OUTPUT), so a parse failure means the workflow is broken — but a bare
// `SyntaxError` surfacing from inside a VitePress config names neither the
// variable nor the value, which is a miserable thing to debug from a CI log.
const parseDocsVersions = (raw: string): VersionMenu => {
  try {
    return JSON.parse(raw) as VersionMenu;
  } catch (cause) {
    throw new Error(
      `DOCS_VERSIONS is not valid JSON — expected {"current":…,"items":[…]}, got: ${raw}`,
      { cause },
    );
  }
};

const DOCS_VERSIONS = process.env.DOCS_VERSIONS
  ? parseDocsVersions(process.env.DOCS_VERSIONS)
  : undefined;
const VERSION_NAV = DOCS_VERSIONS
  ? [{ text: DOCS_VERSIONS.current, items: DOCS_VERSIONS.items }]
  : [];

// https://vitepress.dev/reference/site-config
export default withMermaid(
  defineConfig({
    title: "amqp-contract",
    description: SITE_DESCRIPTION,
    base: BASE,
    lang: "en-US",

    ignoreDeadLinks: [
      // Ignore localhost links as they're for development examples
      /^http:\/\/localhost/,
      // API docs are generated separately and may not exist during build
      /^\/api\//,
      // Ignore relative links in API docs (typedoc-generated cross-references)
      /^\.\/index$/,
      /^\.\/[a-z-]+$/,
    ],

    sitemap: {
      hostname: SITE_URL,
    },

    // Inject canonical URLs and dynamic meta tags for each page to prevent duplicate content issues
    transformPageData(pageData) {
      // Only process markdown files
      if (!pageData.relativePath.endsWith(".md")) {
        return;
      }

      // VitePress provides relativePath without leading slash (e.g., "guide/getting-started.md")
      // Normalize the path by removing any leading slashes just in case
      const normalizedPath = pageData.relativePath.replace(/^\/+/, "");
      // Each version canonicalizes to ITSELF — the beta site must not point its
      // canonical tags at the stable site, or search engines fold the two
      // together and the beta pages drop out of the index entirely.
      const canonicalUrl = `${SITE_URL}${normalizedPath}`
        .replace(/index\.md$/, "")
        .replace(/\.md$/, ".html");

      // Ensure frontmatter and head array exist
      pageData.frontmatter ??= {};
      pageData.frontmatter.head ??= [];

      // Add canonical URL
      pageData.frontmatter.head.push(["link", { rel: "canonical", href: canonicalUrl }]);

      // Add dynamic Open Graph tags
      const pageTitle = pageData.title || pageData.frontmatter.title || "amqp-contract";
      const pageDescription =
        pageData.description || pageData.frontmatter.description || SITE_DESCRIPTION;

      pageData.frontmatter.head.push(
        ["meta", { property: "og:url", content: canonicalUrl }],
        ["meta", { property: "og:title", content: pageTitle }],
        ["meta", { property: "og:description", content: pageDescription }],
      );

      // Add dynamic Twitter Card tags
      pageData.frontmatter.head.push(
        ["meta", { name: "twitter:title", content: pageTitle }],
        ["meta", { name: "twitter:description", content: pageDescription }],
      );
    },

    // Mermaid configuration
    mermaidPlugin: {
      class: "mermaid",
    },

    // `@btravstack/theme` imports `vitepress/theme` (which side-effect-imports the
    // default theme's CSS) plus its own `style.css`. VitePress externalizes the
    // package during the SSR build, so Node's ESM loader hits those `.css` files
    // and throws `ERR_UNKNOWN_FILE_EXTENSION`. Mark it `noExternal` so Vite
    // processes (and strips) the CSS for the server bundle instead.
    vite: {
      ssr: {
        noExternal: ["@btravstack/theme"],
      },
    },

    themeConfig: {
      // https://vitepress.dev/reference/default-theme-config
      logo: { light: "/logo-light.svg", dark: "/logo-dark.svg" },

      nav: [
        { text: "Guides", link: "/guide/getting-started" },
        { text: "API", link: "/api/" },
        { text: "Examples", link: "/examples/" },
        { text: "Changelog", link: "https://github.com/btravstack/amqp-contract/releases" },
        // Back to the btravstack hub (links the docs up to the landing page).
        { text: "btravstack", link: "https://btravstack.github.io/" },
        // Version dropdown — empty unless DOCS_VERSIONS is set by the deploy
        // workflow. Keep this LAST: inject-docs-version-nav.ts places the same
        // dropdown right after the btravstack item on legacy tags, so the
        // picker sits in the same spot on every version of the site.
        ...VERSION_NAV,
      ],

      sidebar: {
        "/guide/": [
          {
            text: "Getting Started",
            items: [
              { text: "Why amqp-contract?", link: "/guide/why-amqp-contract" },
              { text: "Getting Started", link: "/guide/getting-started" },
              { text: "Core Concepts", link: "/guide/core-concepts" },
              { text: "Comparison", link: "/guide/comparison" },
            ],
          },
          {
            text: "Core Usage",
            items: [
              { text: "Defining Contracts", link: "/guide/defining-contracts" },
              { text: "Client Usage", link: "/guide/client-usage" },
              { text: "Worker Usage", link: "/guide/worker-usage" },
              { text: "Error Model", link: "/guide/error-model" },
              {
                text: "Middleware & Interceptors",
                link: "/guide/middleware-and-interceptors",
              },
              { text: "Retry Strategies", link: "/guide/retry-strategies" },
              { text: "Testing", link: "/guide/testing" },
            ],
          },
          {
            text: "Advanced",
            items: [
              { text: "Connection Sharing", link: "/guide/connection-sharing" },
              { text: "Channel Configuration", link: "/guide/channel-configuration" },
              { text: "Bridge Exchanges", link: "/guide/bridge-exchanges" },
              { text: "Message Compression", link: "/guide/message-compression" },
              { text: "Schema Libraries", link: "/guide/schema-libraries" },
              { text: "Performance Tuning", link: "/guide/performance" },
              { text: "AsyncAPI Generation", link: "/guide/asyncapi-generation" },
              {
                text: "Observability",
                collapsed: true,
                items: [
                  { text: "Logging", link: "/guide/logging" },
                  {
                    text: "OpenTelemetry",
                    link: "/guide/opentelemetry-observability",
                  },
                ],
              },
            ],
          },
          {
            text: "Help",
            items: [
              { text: "Troubleshooting", link: "/guide/troubleshooting" },
              { text: "FAQ", link: "/guide/faq" },
              { text: "Upgrading", link: "/guide/upgrading" },
            ],
          },
        ],
        "/api/": [
          {
            text: "Core Packages",
            items: [
              { text: "Overview", link: "/api/" },
              { text: "@amqp-contract/contract", link: "/api/contract/" },
              { text: "@amqp-contract/client", link: "/api/client/" },
              { text: "@amqp-contract/worker", link: "/api/worker/" },
              { text: "@amqp-contract/asyncapi", link: "/api/asyncapi/" },
            ],
          },
          {
            text: "Testing",
            items: [{ text: "@amqp-contract/testing", link: "/api/testing/" }],
          },
        ],
        "/examples/": [
          {
            text: "Examples",
            items: [
              { text: "Overview", link: "/examples/" },
              {
                text: "Basic Order Processing",
                link: "/examples/basic-order-processing",
              },
              {
                text: "Command Pattern",
                link: "/examples/command-pattern",
              },
              {
                text: "AsyncAPI Generation",
                link: "/examples/asyncapi-generation",
              },
            ],
          },
        ],
      },

      socialLinks: [
        { icon: "github", link: "https://github.com/btravstack/amqp-contract" },
        {
          icon: "npm",
          link: "https://www.npmjs.com/package/@amqp-contract/contract",
        },
      ],

      footer: {
        message: "Released under the MIT License.",
        copyright: `Copyright © ${new Date().getFullYear()} Benoit TRAVERS`,
      },

      search: {
        provider: "local",
      },

      editLink: {
        pattern: "https://github.com/btravstack/amqp-contract/edit/main/docs/:path",
        text: "Edit this page on GitHub",
      },
    },

    head: [
      ["link", { rel: "icon", type: "image/svg+xml", href: `${BASE}logo.svg` }],
      // SEO keywords meta tags
      [
        "meta",
        {
          name: "keywords",
          content:
            "AMQP, RabbitMQ, TypeScript, Node.js, messaging, message queue, message broker, type-safe, schema validation, contract-first, AsyncAPI, amqplib, type-safe messaging, schema-based messaging, event-driven architecture, microservices, distributed systems",
        },
      ],
      // Open Graph meta tags for better social sharing and SEO
      ["meta", { property: "og:type", content: "website" }],
      ["meta", { property: "og:site_name", content: "amqp-contract" }],
      ["meta", { property: "og:locale", content: "en_US" }],
      [
        "meta",
        {
          property: "og:image",
          content: `${SITE_URL}og-amqp-contract.png`,
        },
      ],
      ["meta", { property: "og:image:type", content: "image/png" }],
      ["meta", { property: "og:image:width", content: "1200" }],
      ["meta", { property: "og:image:height", content: "630" }],
      [
        "meta",
        {
          property: "og:image:alt",
          content: "amqp-contract — type-safe contracts for AMQP & RabbitMQ",
        },
      ],
      // Twitter Card meta tags
      ["meta", { name: "twitter:card", content: "summary_large_image" }],
      [
        "meta",
        {
          name: "twitter:image",
          content: `${SITE_URL}og-amqp-contract.png`,
        },
      ],
      [
        "meta",
        {
          name: "twitter:image:alt",
          content: "amqp-contract — type-safe contracts for AMQP & RabbitMQ",
        },
      ],
      // Additional SEO meta tags
      ["meta", { name: "author", content: "Benoit TRAVERS" }],
      ["meta", { name: "robots", content: "index, follow" }],
      [
        "meta",
        {
          name: "application-name",
          content: "amqp-contract",
        },
      ],
      // JSON-LD structured data for better SEO
      [
        "script",
        { type: "application/ld+json" },
        JSON.stringify({
          "@context": "https://schema.org",
          "@type": "SoftwareApplication",
          name: "amqp-contract",
          description: SITE_DESCRIPTION,
          applicationCategory: "DeveloperApplication",
          operatingSystem: "Cross-platform",
          offers: {
            "@type": "Offer",
            price: "0",
            priceCurrency: "USD",
          },
          url: SITE_URL,
          author: {
            "@type": "Person",
            name: "Benoit TRAVERS",
          },
          programmingLanguage: {
            "@type": "ComputerLanguage",
            name: "TypeScript",
            url: "https://www.typescriptlang.org/",
          },
          keywords: "AMQP, RabbitMQ, TypeScript, Node.js, messaging, type-safe, schema validation",
        }),
      ],
      // WebSite JSON-LD for proper site name display in Google search
      [
        "script",
        { type: "application/ld+json" },
        JSON.stringify({
          "@context": "https://schema.org",
          "@type": "WebSite",
          name: "amqp-contract",
          url: SITE_URL,
        }),
      ],
      // Organization JSON-LD for logo display in Google search
      [
        "script",
        { type: "application/ld+json" },
        JSON.stringify({
          "@context": "https://schema.org",
          "@type": "Organization",
          name: "amqp-contract",
          url: SITE_URL,
          logo: {
            "@type": "ImageObject",
            url: `${SITE_URL}logo.svg`,
          },
          sameAs: ["https://github.com/btravstack/amqp-contract"],
        }),
      ],
    ],
  }),
);
