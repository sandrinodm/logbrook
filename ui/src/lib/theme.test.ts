import { describe, expect, it } from 'vite-plus/test';

import { THEME_INIT_SCRIPT, THEME_STORAGE_KEY, nextThemePreference } from './theme.ts';

describe('nextThemePreference', () => {
  it('cycles system, light, dark and back', () => {
    expect(nextThemePreference('system')).toBe('light');
    expect(nextThemePreference('light')).toBe('dark');
    expect(nextThemePreference('dark')).toBe('system');
  });
});

describe('THEME_INIT_SCRIPT', () => {
  it('reads the same storage key the toggle writes, and catches storage errors (it runs in a browser test too)', () => {
    expect(THEME_INIT_SCRIPT).toContain(`'${THEME_STORAGE_KEY}'`);
    expect(THEME_INIT_SCRIPT).toContain('catch');
  });
});
