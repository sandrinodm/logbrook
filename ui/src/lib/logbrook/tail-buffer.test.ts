import { describe, expect, it } from 'vite-plus/test';

import { RingBuffer } from './tail-buffer.ts';

describe('RingBuffer', () => {
  it('keeps items oldest first while filling', () => {
    const buffer = new RingBuffer<number>(3);

    buffer.push(1);
    buffer.push(2);

    expect(buffer.toArray()).toEqual([1, 2]);
    expect(buffer.size).toBe(2);
    expect(buffer.dropped).toBe(0);
  });

  it('drops the oldest items once full', () => {
    const buffer = new RingBuffer<number>(3);

    for (let value = 1; value <= 8; value += 1) {
      buffer.push(value);
    }

    expect(buffer.toArray()).toEqual([6, 7, 8]);
    expect(buffer.size).toBe(3);
    expect(buffer.dropped).toBe(5);
  });

  it('hands out copies that later pushes do not change', () => {
    const buffer = new RingBuffer<number>(2);
    buffer.push(1);
    const snapshot = buffer.toArray();

    buffer.push(2);
    buffer.push(3);

    expect(snapshot).toEqual([1]);
  });

  it('clears items and the dropped count', () => {
    const buffer = new RingBuffer<number>(2);
    buffer.push(1);
    buffer.push(2);
    buffer.push(3);

    buffer.clear();

    expect(buffer.toArray()).toEqual([]);
    expect(buffer.dropped).toBe(0);
    buffer.push(9);
    expect(buffer.toArray()).toEqual([9]);
  });

  it('rejects a capacity that is not a positive integer', () => {
    expect(() => new RingBuffer(0)).toThrow(RangeError);
    expect(() => new RingBuffer(1.5)).toThrow(RangeError);
  });
});
