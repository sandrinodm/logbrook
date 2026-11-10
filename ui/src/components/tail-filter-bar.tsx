import { X } from 'lucide-react';
import { useState, type RefObject } from 'react';

import type { AttributeCondition } from '@/lib/logbrook/types';

import { Button } from '@/components/ui/reui-base';
import {
  MAX_ATTRIBUTE_CONDITIONS,
  decodeAttribute,
  encodeAttribute,
  listOf,
  textOf,
  toggleAttribute,
  type SearchPatch,
} from '@/lib/explorer/search';
import { type TailSearch } from '@/lib/logbrook/tail-search';
import { useDebouncedDraft } from '@/lib/use-debounced-draft';

/**
 * Severity choices for the minimum level, from the server's numeric levels.
 */
const LEVEL_OPTIONS = [
  { value: 10, label: 'trace+' },
  { value: 20, label: 'debug+' },
  { value: 30, label: 'info+' },
  { value: 40, label: 'warn+' },
  { value: 50, label: 'error+' },
  { value: 60, label: 'fatal' },
] as const;

/**
 * Class of every input and select in the bar.
 */
const FIELD_CLASS =
  'h-7 rounded border border-border bg-transparent px-2 font-mono text-xs text-foreground placeholder:text-quiet focus-visible:ring-1 focus-visible:ring-ring focus-visible:outline-none';

/**
 * Props of `TailFilterBar`.
 */
type TailFilterBarProps = {
  /**
   * The tail's search params.
   */
  search: TailSearch;

  /**
   * Names of the indexes the credential can read, or `undefined` while they load.
   */
  indexNames: ReadonlyArray<string> | undefined;

  /**
   * The index being followed, or `null` before one is known.
   */
  index: string | null;

  /**
   * Writes a change to the URL, replacing the history entry.
   */
  onPatch: (patch: SearchPatch) => void;

  /**
   * Receives the message box, so the page can focus it with `/`.
   */
  messageInputRef: RefObject<HTMLInputElement | null>;
};

/**
 * The tail's filters: index, sources, service, logger, host, message, minimum level and attribute
 * conditions. They are exactly the filters the API accepts, and they live in the URL.
 */
export function TailFilterBar({
  search,
  indexNames,
  index,
  onPatch,
  messageInputRef,
}: TailFilterBarProps) {
  const attributes = listOf(search.attrs)
    .map(decodeAttribute)
    .filter((condition): condition is AttributeCondition => condition !== null);
  const sourceText = listOf(search.source).join(',');

  return (
    <div className="flex flex-wrap items-center gap-2 border-b border-border px-4 py-2">
      <select
        aria-label="Index"
        className={FIELD_CLASS}
        value={index ?? ''}
        onChange={(event) => onPatch({ index: event.target.value })}>
        {index === null ? <option value="">Loading…</option> : null}
        {(indexNames ?? (index === null ? [] : [index])).map((name) => (
          <option key={name} value={name}>
            {name}
          </option>
        ))}
      </select>
      <DebouncedField
        label="Sources (comma-separated)"
        placeholder="source, other"
        value={sourceText}
        toValue={(draft) => parseSources(draft).join(',')}
        onCommit={(draft) => onPatch({ source: parseSources(draft) })}
      />
      <DebouncedField
        label="Service"
        placeholder="service"
        value={textOf(search.service) ?? ''}
        onCommit={(value) => onPatch({ service: value })}
      />
      <DebouncedField
        label="Logger"
        placeholder="logger"
        value={textOf(search.logger) ?? ''}
        onCommit={(value) => onPatch({ logger: value })}
      />
      <DebouncedField
        label="Host"
        placeholder="host"
        value={textOf(search.host) ?? ''}
        onCommit={(value) => onPatch({ host: value })}
      />
      <DebouncedField
        label="Message contains (case-sensitive)"
        placeholder="message contains (case-sensitive)"
        wide
        inputRef={messageInputRef}
        value={textOf(search.message) ?? ''}
        onCommit={(value) => onPatch({ message: value })}
      />
      <select
        aria-label="Minimum level"
        className={FIELD_CLASS}
        value={search.minLevel ?? ''}
        onChange={(event) =>
          onPatch({ minLevel: event.target.value === '' ? undefined : Number(event.target.value) })
        }>
        <option value="">any level</option>
        {LEVEL_OPTIONS.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
      <AttributeFilters
        conditions={attributes}
        onChange={(next) => onPatch({ attrs: next.map(encodeAttribute) })}
      />
    </div>
  );
}

/**
 * Splits the Sources box into source names: comma-separated, trimmed, without empty entries.
 */
function parseSources(text: string): string[] {
  return text
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item !== '');
}

/**
 * A text input that writes to the URL only after the user stops typing. When the URL value changes
 * from elsewhere (back button, a link), the field follows it; the echo of its own commit does not
 * touch what is typed.
 */
function DebouncedField({
  label,
  placeholder,
  value,
  wide = false,
  inputRef,
  toValue,
  onCommit,
}: {
  label: string;
  placeholder: string;
  value: string;
  wide?: boolean;
  inputRef?: RefObject<HTMLInputElement | null>;
  toValue?: (draft: string) => string;
  onCommit: (value: string) => void;
}) {
  const [draft, setDraft] = useDebouncedDraft({ value, onCommit, toValue });

  return (
    <input
      ref={inputRef}
      aria-label={label}
      placeholder={placeholder}
      className={`${FIELD_CLASS} ${wide ? 'w-64' : 'w-32'}`}
      value={draft}
      onChange={(event) => setDraft(event.target.value)}
    />
  );
}

/**
 * The attribute conditions as removable chips, and a small form to add one. A condition compares
 * one top-level attribute for equality; the type selector decides whether `123` is a string or a
 * number, because the server treats them as different values.
 */
function AttributeFilters({
  conditions,
  onChange,
}: {
  conditions: ReadonlyArray<AttributeCondition>;
  onChange: (next: ReadonlyArray<AttributeCondition>) => void;
}) {
  const [key, setKey] = useState('');
  const [value, setValue] = useState('');
  const [type, setType] = useState<'s' | 'n' | 'b'>('s');
  const candidate = decodeAttribute(`${type}:${key.trim()}=${value}`);
  const full = conditions.length >= MAX_ATTRIBUTE_CONDITIONS;

  /**
   * Adds the typed condition and clears the form.
   */
  const add = () => {
    if (candidate !== null) {
      onChange(toggleAttribute(conditions, candidate));
      setKey('');
      setValue('');
    }
  };

  return (
    <>
      {conditions.map((condition) => (
        <span
          key={condition.key}
          className="inline-flex h-7 items-center gap-1 rounded border border-border bg-secondary px-2 font-mono text-xs">
          {condition.key}={String(condition.value)}
          <button
            type="button"
            aria-label={`Remove filter ${condition.key}`}
            className="cursor-pointer text-quiet hover:text-foreground"
            onClick={() => onChange(conditions.filter((item) => item.key !== condition.key))}>
            <X className="h-3 w-3" />
          </button>
        </span>
      ))}
      <form
        className="flex items-center gap-1"
        onSubmit={(event) => {
          event.preventDefault();
          add();
        }}>
        <input
          aria-label="Attribute key"
          placeholder="attribute"
          className={`${FIELD_CLASS} w-28`}
          value={key}
          onChange={(event) => setKey(event.target.value)}
        />
        <select
          aria-label="Attribute type"
          className={FIELD_CLASS}
          value={type}
          onChange={(event) => setType(event.target.value as 's' | 'n' | 'b')}>
          <option value="s">text</option>
          <option value="n">number</option>
          <option value="b">boolean</option>
        </select>
        <input
          aria-label="Attribute value"
          placeholder="= value"
          className={`${FIELD_CLASS} w-28`}
          value={value}
          onChange={(event) => setValue(event.target.value)}
        />
        <Button type="submit" variant="secondary" size="sm" disabled={candidate === null || full}>
          Add
        </Button>
      </form>
    </>
  );
}
