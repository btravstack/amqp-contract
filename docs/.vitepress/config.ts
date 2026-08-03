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
    // oxlint-disable-next-line unthrown/no-throw -- build-time fail-fast: a broken DOCS_VERSIONS means the deploy workflow is broken
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

// Sitemap URLs of the pre-3.0 redirect tombstones, collected by
// `transformPageData` (which runs for every page before the sitemap is
// generated) and subtracted from the sitemap below. Derived rather than
// hardcoded so a new stub is excluded automatically.
const REDIRECT_URLS = new Set<string>();

// The guide is structured by the four Diátaxis modes (https://diataxis.fr/):
// a learning-oriented Tutorial, task-oriented How-to guides,
// information-oriented Reference, and understanding-oriented Explanation. One
// shared sidebar carries all four so any page can reach any other — a reader
// who arrives at a how-to from a search engine can see the explanation that
// justifies it, and vice versa.
const GUIDE_SIDEBAR = [
  {
    text: "Tutorial",
    items: [
      { text: "Getting started", link: "/tutorial/getting-started" },
      { text: "Adding request/reply", link: "/tutorial/adding-request-reply" },
    ],
  },
  {
    text: "How-to guides",
    items: [
      { text: "Define a contract", link: "/how-to/define-a-contract" },
      { text: "Publish messages", link: "/how-to/publish-messages" },
      { text: "Consume messages", link: "/how-to/consume-messages" },
      { text: "Use request/reply", link: "/how-to/use-request-reply" },
      { text: "Retry failed messages", link: "/how-to/retry-failed-messages" },
      { text: "Route dead letters", link: "/how-to/route-dead-letters" },
      { text: "Bridge domains", link: "/how-to/bridge-domains" },
      { text: "Add middleware", link: "/how-to/add-middleware" },
      { text: "Compress messages", link: "/how-to/compress-messages" },
      { text: "Share connections", link: "/how-to/share-connections" },
      { text: "Configure channels", link: "/how-to/configure-channels" },
      { text: "Add logging", link: "/how-to/add-logging" },
      { text: "Instrument with OpenTelemetry", link: "/how-to/instrument-with-opentelemetry" },
      { text: "Generate AsyncAPI", link: "/how-to/generate-asyncapi" },
      { text: "Test with RabbitMQ", link: "/how-to/test-with-rabbitmq" },
      { text: "Tune performance", link: "/how-to/tune-performance" },
      { text: "Upgrade", link: "/how-to/upgrade" },
      { text: "Troubleshoot", link: "/how-to/troubleshoot" },
    ],
  },
  {
    text: "Reference",
    items: [
      { text: "Error model", link: "/reference/error-model" },
      { text: "Topology options", link: "/reference/topology-options" },
      { text: "Schema libraries", link: "/reference/schema-libraries" },
      { text: "Glossary", link: "/reference/glossary" },
      { text: "FAQ", link: "/reference/faq" },
      { text: "API reference", link: "/api/" },
    ],
  },
  {
    text: "Explanation",
    items: [
      { text: "Why amqp-contract?", link: "/explanation/why-amqp-contract" },
      { text: "Core concepts", link: "/explanation/core-concepts" },
      { text: "Errors as values", link: "/explanation/errors-as-values" },
      { text: "The retry model", link: "/explanation/the-retry-model" },
      { text: "Delivery guarantees", link: "/explanation/delivery-guarantees" },
      { text: "Comparison", link: "/explanation/comparison" },
    ],
  },
  {
    text: "Examples",
    items: [
      { text: "Overview", link: "/examples/" },
      { text: "Basic order processing", link: "/examples/basic-order-processing" },
      { text: "Command pattern", link: "/examples/command-pattern" },
    ],
  },
];

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
      // Keep the redirect tombstones out of the sitemap — they are `noindex`
      // stubs preserving pre-3.0 URLs, not pages we want crawled as content.
      transformItems: (items) => items.filter((item) => !REDIRECT_URLS.has(item.url)),
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

      // Redirect stubs. The 3.0 restructure moved every /guide/ page into a
      // Diátaxis quadrant; a page with `redirect: /how-to/…` in its frontmatter
      // is a tombstone kept so inbound links and search results survive the
      // move. It points its canonical at the DESTINATION (consolidating ranking
      // there rather than competing with it) and carries a meta refresh, which
      // is what actually moves a visitor arriving cold from a search result.
      const redirect = pageData.frontmatter.redirect as string | undefined;
      if (redirect) {
        REDIRECT_URLS.add(normalizedPath.replace(/index\.md$/, "").replace(/\.md$/, ".html"));
        const target = `${SITE_URL}${redirect.replace(/^\/+/, "")}`;
        pageData.frontmatter.head.push(
          ["link", { rel: "canonical", href: target }],
          ["meta", { "http-equiv": "refresh", content: `0; url=${target}` }],
          ["meta", { name: "robots", content: "noindex, follow" }],
        );
        return;
      }

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
        { text: "Guide", link: "/tutorial/getting-started" },
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

      // One shared sidebar across every guide quadrant — see GUIDE_SIDEBAR.
      sidebar: {
        "/tutorial/": GUIDE_SIDEBAR,
        "/how-to/": GUIDE_SIDEBAR,
        "/reference/": GUIDE_SIDEBAR,
        "/explanation/": GUIDE_SIDEBAR,
        "/examples/": GUIDE_SIDEBAR,
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
          {
            text: "Guide",
            items: [
              { text: "Tutorial", link: "/tutorial/getting-started" },
              { text: "How-to guides", link: "/how-to/define-a-contract" },
              { text: "Reference", link: "/reference/error-model" },
              { text: "Explanation", link: "/explanation/why-amqp-contract" },
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
