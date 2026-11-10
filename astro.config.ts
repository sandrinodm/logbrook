import { defineConfig } from "astro/config";
import tailwindcss from "@tailwindcss/vite";
import nimbus, {
  defineConfig as defineNimbusConfig,
} from "@cloudflare/nimbus-docs";
import { tableScroll } from "@cloudflare/nimbus-docs/markdown";

const nimbusConfig = defineNimbusConfig({
  // GitHub Pages project site. The path part lives in Astro's `base` below;
  // a custom domain later only changes `site` and `base`.
  site: "https://sandrinodm.github.io",
  title: "Logbrook",
  description:
    "Self-hosted log search for structured application logs: one Rust binary, DuckDB for recent events, Parquet for archives.",
  locale: "en",
  github: "https://github.com/sandrinodm/logbrook",
  // Pages live on the docs-site branch of the main repository. Nimbus
  // replaces `{path}` with the file's repo path (`src/content/docs/….mdx`).
  editPattern: "https://github.com/sandrinodm/logbrook/edit/docs-site/{path}",
  socialImageAlt: "Logbrook documentation",
  // Origin-root discovery files (/.well-known/*) can't be served from a
  // project page under /logbrook, so leave them out.
  agents: { discovery: false },
  // Top-level order of the rail; each folder's pages fill in automatically.
  sidebar: {
    items: [
      "index",
      { label: "Get started", autogenerate: { directory: "get-started" } },
      { label: "Concepts", autogenerate: { directory: "concepts" } },
      { label: "Send logs", autogenerate: { directory: "send-logs" } },
      { label: "Query logs", autogenerate: { directory: "query" } },
      { label: "Configure", autogenerate: { directory: "configure" } },
      { label: "Operate", autogenerate: { directory: "operate" } },
      { label: "Reference", autogenerate: { directory: "reference" } },
      { label: "AI agents", autogenerate: { directory: "ai-agents" } },
      { label: "Project", autogenerate: { directory: "project" } },
    ],
  },
  // The API reference, served where its route file is (src/pages/api/ → /api).
  // See https://nimbus-docs.com/api-reference/
  // `operations` sets each page URL by operationId. Without it, Nimbus derives
  // the slug from the method (PUT becomes "update", so creating an index would
  // be served at indexes/update). Add an entry when the spec gains an operation.
  api: [
    {
      collection: "api",
      spec: "./src/api/openapi.json",
      routes: {
        convention: "resource-action-v2",
        operations: {
          listIndexes: "indexes/list",
          createIndex: "indexes/create",
          getIndex: "indexes/get",
          deleteIndex: "indexes/delete",
          searchLogs: "logs/search",
          countLogs: "logs/count",
          getLogFacets: "logs/facets",
          getLogHistogram: "logs/histogram",
          ingestLogs: "logs/ingest",
          tailLogs: "logs/tail",
          searchDefaultLogs: "default-index/search",
          countDefaultLogs: "default-index/count",
          getDefaultLogFacets: "default-index/facets",
          getDefaultLogHistogram: "default-index/histogram",
          ingestDefaultLogs: "default-index/ingest",
          tailDefaultLogs: "default-index/tail",
          getHealth: "health",
          getReadiness: "ready",
          getMetrics: "metrics",
          getOpenApiDocument: "openapi",
        },
      },
    },
  ],
});

export default defineConfig({
  // Served at https://sandrinodm.github.io/logbrook/. Write internal links
  // without the base (`/get-started/quickstart/`); Nimbus adds it.
  base: "/logbrook",
  // nimbus:adapter
  output: "static",
  // Tailwind v4 via its Vite plugin (the integration Astro recommends for
  // Tailwind v4 — replaces the PostCSS plugin, which doesn't build under
  // Astro 7's Vite 8 bundler).
  vite: {
    plugins: [tailwindcss()],
  },
  // Hover-prefetch link targets so full-page navigations feel instant without
  // a client-side router.
  prefetch: {
    prefetchAll: true,
    defaultStrategy: "hover",
  },
  integrations: [
    nimbus(nimbusConfig, {
      // Authoring rules are opt-in by design — your repo, your taste. The
      // two below are the load-bearing pair: frontmatter has to validate
      // against the content schema for the page to render properly, and
      // broken internal links are 404s for your readers. Add the others
      // (heading hierarchy, code-block language, style, etc.) when you're
      // ready to enforce them — see `nimbus-docs lint --help`.
      rules: {
        "nimbus/frontmatter-shape": "error",
        "nimbus/internal-link": "error",
      },
      markdown: {
        // Wrap wide tables so they scroll instead of overflowing the page
        // (styled by `.nb-table-scroll` in src/styles/prose.css).
        hastPlugins: [tableScroll()],
        // `## Install {#install}` gives a heading a fixed id. A heading that
        // ends in other `{…}` text, such as `{account_id}` in a .md file, is
        // read as attributes too; remove this to keep it as text.
        features: { headingAttributes: true },
      },
    }),
  ],
});
