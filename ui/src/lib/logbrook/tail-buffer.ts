/**
 * The most events the live tail keeps. Older events are dropped as new ones arrive, so memory stays
 * flat however long the tail runs.
 */
export const TAIL_BUFFER_CAPACITY = 10_000;

/**
 * A fixed-capacity buffer that overwrites its oldest item when full.
 *
 * Pushing is O(1) and allocates nothing. `toArray` copies, so a caller can hold the result as an
 * immutable snapshot while the buffer keeps changing.
 */
export class RingBuffer<T> {
  /**
   * The largest number of items the buffer holds.
   */
  readonly capacity: number;

  /**
   * Backing storage; slots are reused in a circle.
   */
  private slots: Array<T | undefined>;

  /**
   * Index of the oldest item.
   */
  private head = 0;

  /**
   * Number of items held, at most `capacity`.
   */
  private count = 0;

  /**
   * Number of items pushed out to make room since the last `clear`.
   */
  private overwritten = 0;

  constructor(capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new RangeError('Ring buffer capacity must be a positive integer');
    }

    this.capacity = capacity;
    this.slots = Array.from<T | undefined>({ length: capacity });
  }

  /**
   * Number of items currently held.
   */
  get size(): number {
    return this.count;
  }

  /**
   * Number of items dropped because the buffer was full.
   */
  get dropped(): number {
    return this.overwritten;
  }

  /**
   * Adds an item as the newest, dropping the oldest when the buffer is full.
   */
  push(item: T): void {
    if (this.count < this.capacity) {
      this.slots[(this.head + this.count) % this.capacity] = item;
      this.count += 1;
      return;
    }

    this.slots[this.head] = item;
    this.head = (this.head + 1) % this.capacity;
    this.overwritten += 1;
  }

  /**
   * Returns a new array of the held items, oldest first.
   */
  toArray(): Array<T> {
    const end = this.head + this.count;
    if (end <= this.capacity) {
      return this.slots.slice(this.head, end) as Array<T>;
    }

    const wrapped = this.slots.slice(0, end - this.capacity);
    return this.slots.slice(this.head).concat(wrapped) as Array<T>;
  }

  /**
   * Empties the buffer and resets the dropped count.
   */
  clear(): void {
    this.slots = Array.from<T | undefined>({ length: this.capacity });
    this.head = 0;
    this.count = 0;
    this.overwritten = 0;
  }
}
