import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { gzipSync } from 'node:zlib';

/**
 * One module in `bundle-stats.json`.
 */
type StatsModule = {
  /**
   * Module path as the bundler saw it.
   */
  readonly id: string;

  /**
   * Rendered size in bytes, before minification.
   */
  readonly bytes: number;
};

/**
 * One chunk in `bundle-stats.json`, as written by the `bundleStats` plugin of `vite.config.ts`.
 */
type StatsChunk = {
  /**
   * Output file name below `.output/public`.
   */
  readonly file: string;

  /**
   * Whether the chunk is the entry chunk.
   */
  readonly isEntry: boolean;

  /**
   * Chunks it imports statically.
   */
  readonly imports: readonly string[];

  /**
   * Chunks it imports dynamically.
   */
  readonly dynamicImports: readonly string[];

  /**
   * Size of the minified chunk in bytes.
   */
  readonly bytes: number;

  /**
   * The modules it contains.
   */
  readonly modules: readonly StatsModule[];
};

/**
 * The package a module belongs to (`effect`, `@base-ui/react`), or `app` for the app's own code.
 */
function packageOf(id: string): string {
  const marker = 'node_modules/';
  const at = id.lastIndexOf(marker);
  if (at < 0) {
    return id.includes('\0') ? 'virtual' : 'app';
  }

  const parts = id.slice(at + marker.length).split('/');
  return parts[0]?.startsWith('@') ? `${parts[0]}/${parts[1]}` : (parts[0] ?? 'unknown');
}

/**
 * Prints the rendered size per package over all client chunks, and the minified and gzipped size of
 * every chunk. Run `pnpm build:analyze` first. With a chunk-name fragment argument it also lists
 * the largest modules of the matching chunks.
 */
function main(): void {
  const root = resolve(import.meta.dirname, '..');
  const stats = JSON.parse(
    readFileSync(resolve(root, 'bundle-stats.json'), 'utf8'),
  ) as StatsChunk[];
  const detail = process.argv[2];

  const perPackage = new Map<string, number>();
  for (const chunk of stats) {
    for (const module of chunk.modules) {
      const name = packageOf(module.id);
      perPackage.set(name, (perPackage.get(name) ?? 0) + module.bytes);
    }
  }

  console.log('Rendered bytes per package, all client chunks:');
  for (const [name, bytes] of [...perPackage].sort((a, b) => b[1] - a[1]).slice(0, 25)) {
    console.log(`  ${(bytes / 1000).toFixed(1).padStart(8)} kB  ${name}`);
  }

  console.log('\nChunks (rendered kB, gzip kB):');
  for (const chunk of [...stats].sort((a, b) => b.bytes - a.bytes)) {
    const code = readFileSync(resolve(root, '.output', 'public', chunk.file));
    const gzip = gzipSync(code).length;
    console.log(
      `  ${(chunk.bytes / 1000).toFixed(1).padStart(8)}  ${(gzip / 1000).toFixed(1).padStart(7)}  ${chunk.file}${chunk.isEntry ? '  (entry)' : ''}`,
    );
    if (detail !== undefined && chunk.file.includes(detail)) {
      for (const module of [...chunk.modules].sort((a, b) => b.bytes - a.bytes).slice(0, 15)) {
        console.log(
          `      ${(module.bytes / 1000).toFixed(1).padStart(8)}  ${module.id.split('node_modules/').at(-1)}`,
        );
      }
    }
  }
}

main();
