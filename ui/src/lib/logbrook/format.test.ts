import { describe, expect, it } from 'vite-plus/test';

import {
  formatBytes,
  formatCount,
  formatRate,
  formatRetention,
  formatSeconds,
  usagePercent,
} from './format.ts';

describe('formatBytes', () => {
  it.each([
    [0, '0 B'],
    [999, '999 B'],
    [1000, '1.00 KB'],
    [10_485_760, '10.5 MB'],
    [604_800_000, '605 MB'],
    [2_500_000_000_000, '2.50 TB'],
    [5_000_000_000_000_000, '5000 TB'],
  ])('formats %d as %s', (bytes, expected) => {
    expect(formatBytes(bytes)).toBe(expected);
  });

  it('prints an en dash for an unknown size, not zero', () => {
    expect(formatBytes(undefined)).toBe('–');
    expect(formatBytes(null)).toBe('–');
  });
});

describe('formatRetention', () => {
  it.each([
    [604_800_000, '7d'],
    [129_600_000, '36h'],
    [90_000, '90s'],
    [120_000, '2m'],
    [1500, '1500 ms'],
    [100_000_000, '1.2d'],
  ])('formats %d ms as %s', (ms, expected) => {
    expect(formatRetention(ms)).toBe(expected);
  });

  it('prints an en dash when the token does not receive retention', () => {
    expect(formatRetention(undefined)).toBe('–');
  });
});

describe('counts, rates and durations', () => {
  it('groups digits', () => {
    expect(formatCount(22_106_782)).toBe('22,106,782');
    expect(formatCount(null)).toBe('–');
  });

  it('keeps a decimal for small rates only', () => {
    expect(formatRate(0.25)).toBe('0.3');
    expect(formatRate(1234.4)).toBe('1,234');
    expect(formatRate(null)).toBe('–');
  });

  it('formats seconds as milliseconds below one second', () => {
    expect(formatSeconds(0.0042)).toBe('4.2 ms');
    expect(formatSeconds(0.25)).toBe('250 ms');
    expect(formatSeconds(1.234)).toBe('1.2 s');
    expect(formatSeconds(Number.POSITIVE_INFINITY)).toBe('> 5 s');
    expect(formatSeconds(null)).toBe('–');
  });
});

describe('usagePercent', () => {
  it('is the share of the limit with one decimal', () => {
    expect(usagePercent(250_000, 1_000_000)).toBe(25);
    expect(usagePercent(333, 1000)).toBe(33.3);
  });

  it('is null without a limit or a size', () => {
    expect(usagePercent(10, null)).toBeNull();
    expect(usagePercent(undefined, 10)).toBeNull();
    expect(usagePercent(10, 0)).toBeNull();
  });
});
