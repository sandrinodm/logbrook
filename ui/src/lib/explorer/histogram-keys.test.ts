import { describe, expect, it } from 'vite-plus/test';

import type { HistogramKeyAction, HistogramKeyState } from './histogram-keys.ts';

import { applyHistogramKey, hasKeySelection, selectedBuckets } from './histogram-keys.ts';

/**
 * A state action, narrowed, or a failure naming what came back.
 */
function stateOf(action: HistogramKeyAction): HistogramKeyState {
  if (action.kind !== 'state') {
    throw new Error(`Expected a state change, got ${action.kind}`);
  }

  return action.state;
}

describe('applyHistogramKey', () => {
  it('puts the cursor on the newest bucket on the first movement key', () => {
    expect(stateOf(applyHistogramKey(null, 'ArrowLeft', false, 10))).toEqual({
      cursor: 9,
      anchor: null,
    });
    expect(stateOf(applyHistogramKey(null, 'ArrowRight', false, 10))).toEqual({
      cursor: 9,
      anchor: null,
    });
  });

  it('moves one bucket and stops at the edges', () => {
    expect(
      stateOf(applyHistogramKey({ cursor: 4, anchor: null }, 'ArrowLeft', false, 10)).cursor,
    ).toBe(3);
    expect(
      stateOf(applyHistogramKey({ cursor: 0, anchor: null }, 'ArrowLeft', false, 10)).cursor,
    ).toBe(0);
    expect(
      stateOf(applyHistogramKey({ cursor: 9, anchor: null }, 'ArrowRight', false, 10)).cursor,
    ).toBe(9);
  });

  it('jumps with Home and End', () => {
    expect(stateOf(applyHistogramKey({ cursor: 4, anchor: null }, 'Home', false, 10)).cursor).toBe(
      0,
    );
    expect(stateOf(applyHistogramKey({ cursor: 4, anchor: null }, 'End', false, 10)).cursor).toBe(
      9,
    );
  });

  it('starts a selection at the bucket the cursor was on when Shift is held', () => {
    expect(stateOf(applyHistogramKey({ cursor: 4, anchor: null }, 'ArrowRight', true, 10))).toEqual(
      {
        cursor: 5,
        anchor: 4,
      },
    );
  });

  it('keeps the anchor while the selection grows or shrinks', () => {
    let state: HistogramKeyState = { cursor: 4, anchor: null };
    state = stateOf(applyHistogramKey(state, 'ArrowRight', true, 10));
    state = stateOf(applyHistogramKey(state, 'ArrowRight', true, 10));
    expect(state).toEqual({ cursor: 6, anchor: 4 });

    state = stateOf(applyHistogramKey(state, 'ArrowLeft', true, 10));
    state = stateOf(applyHistogramKey(state, 'ArrowLeft', true, 10));
    state = stateOf(applyHistogramKey(state, 'ArrowLeft', true, 10));
    expect(state).toEqual({ cursor: 3, anchor: 4 });
    expect(selectedBuckets(state)).toEqual({ first: 3, last: 4 });
  });

  it('drops the selection when the cursor moves without Shift', () => {
    expect(stateOf(applyHistogramKey({ cursor: 6, anchor: 4 }, 'ArrowLeft', false, 10))).toEqual({
      cursor: 5,
      anchor: null,
    });
  });

  it('zooms to the selection, or to the cursor bucket when there is none', () => {
    expect(applyHistogramKey({ cursor: 3, anchor: 6 }, 'Enter', false, 10)).toEqual({
      kind: 'zoom',
      first: 3,
      last: 6,
    });
    expect(applyHistogramKey({ cursor: 3, anchor: null }, 'Enter', false, 10)).toEqual({
      kind: 'zoom',
      first: 3,
      last: 3,
    });
  });

  it('ignores Enter and Escape when the keyboard has not touched the chart', () => {
    expect(applyHistogramKey(null, 'Enter', false, 10)).toEqual({ kind: 'ignore' });
    expect(applyHistogramKey(null, 'Escape', false, 10)).toEqual({ kind: 'ignore' });
  });

  it('clears on Escape', () => {
    expect(applyHistogramKey({ cursor: 3, anchor: 6 }, 'Escape', false, 10)).toEqual({
      kind: 'clear',
    });
  });

  it('ignores other keys and an empty chart', () => {
    expect(applyHistogramKey({ cursor: 3, anchor: null }, 'a', false, 10)).toEqual({
      kind: 'ignore',
    });
    expect(applyHistogramKey(null, 'ArrowLeft', false, 0)).toEqual({ kind: 'ignore' });
  });

  it('clamps a cursor left beyond the end by a chart that got narrower', () => {
    expect(
      stateOf(applyHistogramKey({ cursor: 40, anchor: null }, 'ArrowLeft', false, 10)).cursor,
    ).toBe(8);
  });
});

describe('hasKeySelection', () => {
  it('is true only for a selection started with Shift', () => {
    expect(hasKeySelection(null)).toBe(false);
    expect(hasKeySelection({ cursor: 2, anchor: null })).toBe(false);
    expect(hasKeySelection({ cursor: 2, anchor: 1 })).toBe(true);
  });
});
