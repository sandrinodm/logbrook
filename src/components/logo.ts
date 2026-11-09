/**
 * The site's logo, drawn once for every place it appears: the header and the
 * docs layout (Logo.astro) and the social cards (src/pages/og/_card.ts).
 * A single-colour mark of three log lines; each place fills it with its own
 * text colour.
 */
export const logo = {
  /** The drawing's box; callers size it by height and keep the ratio. */
  width: 64,
  height: 64,
  paths: [
    "M9 10h46a5 5 0 0 1 0 10H9a5 5 0 0 1 0-10Z",
    "M9 27h26a5 5 0 0 1 0 10H9a5 5 0 0 1 0-10Z",
    "M9 44h36a5 5 0 0 1 0 10H9a5 5 0 0 1 0-10Z",
  ],
};
