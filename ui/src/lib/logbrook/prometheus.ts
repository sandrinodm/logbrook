/**
 * One sample line of the Prometheus text format: a series name, its labels and its value.
 */
export type PrometheusSample = {
  /**
   * Series name, for example `logbrook_http_requests_total`. Histogram series keep their `_bucket`,
   * `_sum` and `_count` suffixes.
   */
  readonly name: string;

  /**
   * Label names and values; empty for a series without labels.
   */
  readonly labels: Readonly<Record<string, string>>;

  /**
   * The sample value. `NaN` and the infinities are kept as such.
   */
  readonly value: number;
};

/**
 * Parses the text exposition format (version 0.0.4) into samples.
 *
 * Comment lines (`# HELP`, `# TYPE`) and blank lines are skipped, and so is any line that does not
 * parse, so one odd series cannot hide the rest. A trailing timestamp is ignored. Label values may
 * contain the escapes `\\`, `\"` and `\n`.
 */
export function parsePrometheusText(text: string): ReadonlyArray<PrometheusSample> {
  const samples: PrometheusSample[] = [];
  for (const rawLine of text.split('\n')) {
    const line = rawLine.trim();
    if (line === '' || line.startsWith('#')) {
      continue;
    }

    const sample = parseLine(line);
    if (sample !== null) {
      samples.push(sample);
    }
  }

  return samples;
}

/**
 * Returns the samples with the given series name.
 */
export function samplesNamed(
  samples: ReadonlyArray<PrometheusSample>,
  name: string,
): ReadonlyArray<PrometheusSample> {
  return samples.filter((sample) => sample.name === name);
}

/**
 * Returns the value of the first sample with the given name and no label requirement, or `null`
 * when the series is absent.
 */
export function valueOf(samples: ReadonlyArray<PrometheusSample>, name: string): number | null {
  return samples.find((sample) => sample.name === name)?.value ?? null;
}

/**
 * Parses one sample line, or returns `null` when it is malformed.
 */
function parseLine(line: string): PrometheusSample | null {
  const nameMatch = /^[a-zA-Z_:][a-zA-Z0-9_:]*/.exec(line);
  if (nameMatch === null) {
    return null;
  }

  const name = nameMatch[0];
  let rest = line.slice(name.length);
  let labels: Record<string, string> = {};
  if (rest.startsWith('{')) {
    const parsed = parseLabels(rest);
    if (parsed === null) {
      return null;
    }

    labels = parsed.labels;
    rest = parsed.rest;
  }

  const value = parseValue(rest);
  return value === null ? null : { name, labels, value };
}

/**
 * Parses a `{name="value",...}` block at the start of `text`. Returns the labels and the text after
 * the closing brace, or `null` when the block is malformed.
 */
function parseLabels(text: string): { labels: Record<string, string>; rest: string } | null {
  const labels: Record<string, string> = {};
  let position = 1;
  while (position < text.length) {
    if (text[position] === '}') {
      return { labels, rest: text.slice(position + 1) };
    }

    const pair = /^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"/.exec(text.slice(position));
    if (pair === null || pair[1] === undefined) {
      return null;
    }

    const value = readQuoted(text, position + pair[0].length);
    if (value === null) {
      return null;
    }

    labels[pair[1]] = value.text;
    position = skipComma(text, value.end);
  }

  return null;
}

/**
 * Reads a quoted label value starting just after the opening quote. Returns the unescaped text and
 * the index after the closing quote, or `null` when the quote never closes.
 */
function readQuoted(text: string, start: number): { text: string; end: number } | null {
  let result = '';
  for (let position = start; position < text.length; position += 1) {
    const char = text[position];
    if (char === '"') {
      return { text: result, end: position + 1 };
    }

    if (char === '\\') {
      position += 1;
      result += unescape(text[position]);
    } else {
      result += char;
    }
  }

  return null;
}

/**
 * Resolves the character after a backslash in a label value. Unknown escapes keep the character.
 */
function unescape(char: string | undefined): string {
  return char === 'n' ? '\n' : (char ?? '');
}

/**
 * Returns the index after optional whitespace and one comma that follow a label value.
 */
function skipComma(text: string, from: number): number {
  let position = from;
  while (text[position] === ' ') {
    position += 1;
  }

  return text[position] === ',' ? position + 1 : position;
}

/**
 * Parses the value that follows a series name and labels: the number, then an optional timestamp.
 */
function parseValue(rest: string): number | null {
  const [token] = rest.trim().split(/\s+/);
  if (token === undefined || token === '') {
    return null;
  }

  const lowered = token.toLowerCase();
  if (lowered === 'nan') {
    return Number.NaN;
  }

  if (lowered === '+inf' || lowered === 'inf') {
    return Number.POSITIVE_INFINITY;
  }

  if (lowered === '-inf') {
    return Number.NEGATIVE_INFINITY;
  }

  const value = Number(token);
  return Number.isNaN(value) ? null : value;
}
