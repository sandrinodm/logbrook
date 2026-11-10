/**
 * The site's logo, drawn once for every place it appears: the header and the
 * docs layout (Logo.astro) and the social cards (src/pages/og/_card.ts).
 * A single-colour mark of three waves, the one in the architecture diagram;
 * each place strokes it with its own text colour. public/favicon.svg is a
 * copy, so change it together with this file.
 */
export const logo = {
  /** The drawing's box; callers size it by height and keep the ratio. */
  width: 64,
  height: 64,
  /** The paths are centre lines, drawn as round-capped strokes of this width. */
  strokeWidth: 6.5,
  paths: [
    "M8 18C15 12 23 12 32 18S49 24 56 18",
    "M8 32C15 26 23 26 32 32S49 38 56 32",
    "M8 46C15 40 23 40 32 46S49 52 56 46",
  ],
};
