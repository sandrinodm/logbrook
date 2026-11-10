import { useState } from 'react';

import type { IndexMutationOutcome } from '@/functions/indexes.functions';
import type { IndexInfo } from '@/lib/logbrook/types';

import { CreateIndexDialog, DeleteIndexDialog } from '@/components/index-dialogs';
import { BaseProgress } from '@/components/ui/base-progress';
import { Button } from '@/components/ui/reui-base';
import { hasIndexDetail } from '@/lib/logbrook/capabilities';
import { formatBytes, formatRetention, usagePercent } from '@/lib/logbrook/format';

/**
 * Index that exists on every server and can never be deleted.
 */
const PROTECTED_INDEX = 'default';

/**
 * Fill of a usage bar from this percentage upward, which flags an index close to its size limit.
 */
const NEAR_LIMIT_PERCENT = 85;

/**
 * The storage components Logbrook reports per index, in the order of the table columns, with the
 * fill that draws each in the composition bar.
 */
const COMPONENTS = [
  { key: 'database_bytes', label: 'Database', fill: 'bg-foreground/80' },
  { key: 'wal_bytes', label: 'WAL', fill: 'bg-foreground/55' },
  { key: 'archive_bytes', label: 'Archive', fill: 'bg-foreground/35' },
  { key: 'temp_bytes', label: 'Temp', fill: 'bg-foreground/20' },
  { key: 'other_bytes', label: 'Other', fill: 'bg-foreground/10' },
] as const;

/**
 * Props of the Indexes view.
 */
type IndexesStorageViewProps = {
  /**
   * The indexes to list. Entries without sizes come from a source-restricted read token.
   */
  readonly indexes: ReadonlyArray<IndexInfo>;

  /**
   * Whether the server has an admin token, which enables create and delete.
   */
  readonly canManage: boolean;

  /**
   * Creates an index and resolves with the outcome.
   */
  readonly onCreateIndex: (name: string) => Promise<IndexMutationOutcome>;

  /**
   * Deletes an index and resolves with the outcome.
   */
  readonly onDeleteIndex: (name: string) => Promise<IndexMutationOutcome>;

  /**
   * Opens the explorer on an index.
   */
  readonly onExploreIndex: (name: string) => void;
};

/**
 * Table of indexes with the size of each storage component, the size against its limit, retention
 * and readiness, plus explore, create and delete actions. Create and delete need an admin token;
 * without one the view says so. A source-restricted read token only learns names, and the view
 * shows that instead of empty numbers.
 */
export function IndexesStorageView({
  indexes,
  canManage,
  onCreateIndex,
  onDeleteIndex,
  onExploreIndex,
}: IndexesStorageViewProps) {
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);
  const detailed = indexes.filter(hasIndexDetail);
  const namesOnly = indexes.length > 0 && detailed.length === 0;
  const totalBytes = detailed.reduce((sum, index) => sum + (index.size_bytes ?? 0), 0);

  return (
    <div className="flex-1 overflow-y-auto bg-background">
      <div className="flex flex-wrap items-center justify-between gap-4 border-b border-border px-6 py-3">
        <div className="flex flex-wrap items-center gap-6 font-mono text-xs tabular-nums">
          <Figure label="indexes" value={String(indexes.length)} />
          {namesOnly ? null : <Figure label="total" value={formatBytes(totalBytes)} />}
        </div>

        {canManage ? (
          <CreateIndexDialog
            existingNames={indexes.map((index) => index.name)}
            onCreate={onCreateIndex}
          />
        ) : (
          <p className="text-[11px] text-quiet">
            Creating and deleting indexes needs an admin token, and this server has none: the
            operator can set LOGBROOK_ADMIN_TOKEN.
          </p>
        )}
      </div>

      {namesOnly ? <NamesOnlyNotice /> : null}

      <div className="overflow-x-auto">
        <table className="w-full border-collapse text-left font-mono text-xs tabular-nums">
          <thead>
            <tr className="border-b border-border bg-card/40 text-[10px] tracking-wider text-quiet uppercase">
              <th className="px-6 py-2.5 font-normal">Index</th>
              <th className="px-3 py-2.5 font-normal">Ready</th>
              <th className="px-3 py-2.5 font-normal">Retention</th>
              <th className="w-56 px-3 py-2.5 font-normal">Size / limit</th>
              {COMPONENTS.map((component) => (
                <th key={component.key} className="px-3 py-2.5 font-normal">
                  {component.label}
                </th>
              ))}
              <th className="px-6 py-2.5 text-right font-normal">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border/60">
            {indexes.map((index) => (
              <IndexRow
                key={index.name}
                index={index}
                canManage={canManage}
                onExplore={onExploreIndex}
                onDelete={setDeleteTarget}
              />
            ))}
          </tbody>
        </table>
      </div>

      {indexes.length === 0 ? (
        <p className="px-6 py-8 text-center text-xs text-muted-foreground">
          This token cannot reach any index.
        </p>
      ) : null}

      <DeleteIndexDialog
        target={deleteTarget}
        onDelete={onDeleteIndex}
        onClose={() => setDeleteTarget(null)}
      />
    </div>
  );
}

/**
 * A label and a value in the summary strip.
 */
function Figure({ label, value }: { readonly label: string; readonly value: string }) {
  return (
    <div>
      <span className="text-quiet">{label} </span>
      <span className="font-medium text-foreground">{value}</span>
    </div>
  );
}

/**
 * Explains why the table has no sizes: Logbrook hides them from source-restricted read tokens so
 * they cannot learn how much data other sources store.
 */
function NamesOnlyNotice() {
  return (
    <p className="border-b border-border bg-card/40 px-6 py-3 text-xs text-muted-foreground">
      Logbrook returns index names only to a read token that is restricted to particular sources, so
      sizes, retention and readiness are not shown. The operator can configure an unrestricted read
      token or an admin token to show them.
    </p>
  );
}

/**
 * One table row: the index name, readiness, retention, size against limit, each storage component
 * and the row actions.
 */
function IndexRow({
  index,
  canManage,
  onExplore,
  onDelete,
}: {
  readonly index: IndexInfo;
  readonly canManage: boolean;
  readonly onExplore: (name: string) => void;
  readonly onDelete: (name: string) => void;
}) {
  const detailed = hasIndexDetail(index);
  return (
    <tr className="hover:bg-accent/40">
      <th scope="row" className="px-6 py-3.5 text-left font-medium text-foreground">
        {index.name}
      </th>
      <td className="px-3 py-3.5">
        <Readiness ready={index.ready} />
      </td>
      <td className="px-3 py-3.5 text-muted-foreground">{formatRetention(index.retention_ms)}</td>
      <td className="px-3 py-3.5">{detailed ? <SizeCell index={index} /> : <Missing />}</td>
      {COMPONENTS.map((component) => (
        <td key={component.key} className="px-3 py-3.5 text-muted-foreground">
          {detailed ? formatBytes(index[component.key]) : <Missing />}
        </td>
      ))}
      <td className="px-6 py-3.5 text-right font-sans">
        <div className="inline-flex items-center gap-2">
          <Button variant="outline" size="xs" onClick={() => onExplore(index.name)}>
            Explore
          </Button>
          <DeleteAction name={index.name} canManage={canManage} onDelete={onDelete} />
        </div>
      </td>
    </tr>
  );
}

/**
 * The delete button, or the reason there is none: `default` is protected, and without an admin
 * token nothing can be deleted. Indexes declared in the server configuration are also protected,
 * but the API does not say which, so the server's refusal explains that case.
 */
function DeleteAction({
  name,
  canManage,
  onDelete,
}: {
  readonly name: string;
  readonly canManage: boolean;
  readonly onDelete: (name: string) => void;
}) {
  if (!canManage) {
    return null;
  }

  if (name === PROTECTED_INDEX) {
    return <span className="px-1.5 text-[11px] text-quiet">protected</span>;
  }

  return (
    <Button
      variant="ghost"
      size="xs"
      onClick={() => onDelete(name)}
      className="text-muted-foreground hover:text-[#e05263]">
      Delete
    </Button>
  );
}

/**
 * Readiness badge: ready, unavailable, or an en dash when the token does not see it.
 */
function Readiness({ ready }: { readonly ready: boolean | undefined }) {
  if (ready === undefined) {
    return <Missing />;
  }

  return ready ? (
    <span className="text-foreground">ready</span>
  ) : (
    <span className="text-[#e05263]">unavailable</span>
  );
}

/**
 * Placeholder for a value the token does not receive.
 */
function Missing() {
  return <span className="text-quiet">–</span>;
}

/**
 * Total size with its limit. An index with a limit gets a progress bar against it; one without gets
 * a bar split by storage component.
 */
function SizeCell({ index }: { readonly index: IndexInfo }) {
  const percent = usagePercent(index.size_bytes, index.max_size_bytes);
  const near = percent !== null && percent >= NEAR_LIMIT_PERCENT;
  return (
    <div className="space-y-1">
      <div className="flex justify-between text-[11px]">
        <span className={near ? 'text-[#d4973b]' : 'text-foreground'}>
          {formatBytes(index.size_bytes)}
        </span>
        <span className="text-quiet">
          {percent === null ? 'no limit' : `${formatBytes(index.max_size_bytes)} (${percent}%)`}
        </span>
      </div>
      {percent === null ? (
        <CompositionBar index={index} />
      ) : (
        <BaseProgress
          value={percent}
          max={100}
          indicatorClassName={near ? 'bg-[#d4973b]' : 'bg-foreground/65'}
        />
      )}
    </div>
  );
}

/**
 * A thin bar split into the five storage components, each as wide as its share of the total. It is
 * decorative: the same numbers are in the columns beside it.
 */
function CompositionBar({ index }: { readonly index: IndexInfo }) {
  const total = index.size_bytes ?? 0;
  return (
    <div
      className="flex h-1 w-full overflow-hidden rounded-full bg-secondary"
      aria-hidden="true"
      title={COMPONENTS.map((c) => `${c.label} ${formatBytes(index[c.key])}`).join(', ')}>
      {COMPONENTS.map((component) => (
        <div
          key={component.key}
          className={component.fill}
          style={{ width: `${total === 0 ? 0 : ((index[component.key] ?? 0) / total) * 100}%` }}
        />
      ))}
    </div>
  );
}
