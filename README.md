# Logbrook documentation site

This orphan branch (`docs-site`) holds the source of the Logbrook documentation site,
published at <https://sandrinodm.github.io/logbrook/>. It is built with
[Nimbus](https://nimbus-docs.com/) (Astro) and never merges into `main`.

## Work on it

```sh
git worktree add ../logbrook-docs docs-site   # from a Logbrook checkout
cd ../logbrook-docs
npm ci
npm run dev                                   # http://localhost:4321/logbrook/
```

Pages live in `src/content/docs/`; the folder tree is the URL structure and the sidebar.
`sidebar.order` in frontmatter orders pages within a group, and the top-level groups are
listed in `astro.config.ts`. Components such as `<Steps>`, `<Tabs>` and `<Aside>` are
available in every `.mdx` file without an import; see `src/components.ts`.

The API reference at `/api/` is generated from `src/api/openapi.json`. The deploy workflow
replaces that file with `src/openapi.json` from `main` before each build, so update the
spec on `main`, not here.

Before you push:

```sh
npx @cloudflare/nimbus-docs check
npm run build
npx @cloudflare/nimbus-docs check   # again, to check links in the built site
```

## Deploy

`.github/workflows/pages.yml` builds every push and pull request to `docs-site` and deploys
pushes to GitHub Pages. Run it by hand from the Actions tab to pick up a new API spec from
`main`. One-time repository setup:

1. Settings → Pages → Build and deployment → Source: **GitHub Actions**.
2. Settings → Environments → `github-pages` → Deployment branches and tags: allow
   **`docs-site`**. Without this, deploys fail with "Branch docs-site is not allowed to
   deploy to github-pages due to environment protection rules".

`AGENT.md` has the Nimbus conventions for coding agents working on this branch.
