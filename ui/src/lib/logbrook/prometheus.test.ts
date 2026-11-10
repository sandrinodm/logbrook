import { describe, expect, it } from 'vite-plus/test';

import { parsePrometheusText, samplesNamed, valueOf } from './prometheus.ts';

describe('parsePrometheusText', () => {
  it('reads plain, labelled and timestamped samples and skips comments and blank lines', () => {
    const text = [
      '# HELP logbrook_ready Storage readiness',
      '# TYPE logbrook_ready gauge',
      'logbrook_ready 1',
      '',
      'logbrook_storage_size_bytes{index="payments"} 2048',
      'logbrook_http_overload_total{cause="admission",extra="x"} 3 1700000000000',
    ].join('\n');

    expect(parsePrometheusText(text)).toEqual([
      { name: 'logbrook_ready', labels: {}, value: 1 },
      { name: 'logbrook_storage_size_bytes', labels: { index: 'payments' }, value: 2048 },
      {
        name: 'logbrook_http_overload_total',
        labels: { cause: 'admission', extra: 'x' },
        value: 3,
      },
    ]);
  });

  it('unescapes label values and accepts commas and braces inside them', () => {
    const [sample] = parsePrometheusText('m{a="x\\"y\\\\z\\nw",b="{1,2}"} 7');

    expect(sample?.labels).toEqual({ a: 'x"y\\z\nw', b: '{1,2}' });
    expect(sample?.value).toBe(7);
  });

  it('keeps NaN and the infinities, which DuckDB progress gauges use', () => {
    const samples = parsePrometheusText('a NaN\nb +Inf\nc -Inf\nd{le="+Inf"} 1e3');

    expect(samples.map((sample) => sample.value)).toEqual([
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY,
      1000,
    ]);
    expect(samples[3]?.labels).toEqual({ le: '+Inf' });
  });

  it('drops malformed lines without losing the good ones', () => {
    const text = [
      'good 1',
      'no_value',
      '9bad 2',
      'open{a="x 3',
      'bad_value abc',
      'also_good 2',
    ].join('\n');

    expect(parsePrometheusText(text).map((sample) => sample.name)).toEqual(['good', 'also_good']);
  });

  it('copes with CRLF line endings', () => {
    expect(parsePrometheusText('a 1\r\nb 2\r\n')).toHaveLength(2);
  });
});

describe('samplesNamed and valueOf', () => {
  const samples = parsePrometheusText('a{i="1"} 1\na{i="2"} 2\nb 3');

  it('selects by name', () => {
    expect(samplesNamed(samples, 'a')).toHaveLength(2);
    expect(valueOf(samples, 'b')).toBe(3);
  });

  it('returns null for an absent series', () => {
    expect(valueOf(samples, 'missing')).toBeNull();
  });
});
