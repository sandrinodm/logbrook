/**
 * One parsed server-sent event.
 */
export type SseFrame = {
  /**
   * The `event:` field, or `message` when absent.
   */
  readonly event: string;

  /**
   * The `id:` field, if present.
   */
  readonly id: string | null;

  /**
   * The `data:` lines joined with newlines.
   */
  readonly data: string;
};

/**
 * Reads server-sent events from a response body, one frame at a time. Comments (keepalives) are
 * skipped.
 */
export type SseReader = {
  /**
   * Resolves with the next frame, or `null` when the stream ends. Rejects when the stream fails,
   * for example after an abort.
   */
  readonly next: () => Promise<SseFrame | null>;
};

/**
 * Wraps a response body in an `SseReader`.
 */
export function readSse(response: Response): SseReader {
  if (response.body === null) {
    throw new Error('Response has no body');
  }

  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = '';
  return {
    next: async () => {
      for (;;) {
        const separator = buffer.indexOf('\n\n');
        if (separator >= 0) {
          const block = buffer.slice(0, separator);
          buffer = buffer.slice(separator + 2);
          const frame = parseBlock(block);
          if (frame !== null) {
            return frame;
          }

          continue;
        }

        const chunk = await reader.read();
        if (chunk.done) {
          return null;
        }

        buffer += chunk.value.replaceAll('\r\n', '\n');
      }
    },
  };
}

/**
 * Parses one blank-line-delimited block, or returns `null` for a comment-only block.
 */
function parseBlock(block: string): SseFrame | null {
  let event = 'message';
  let id: string | null = null;
  const data: string[] = [];
  for (const line of block.split('\n')) {
    if (line.startsWith('event:')) {
      event = line.slice('event:'.length).trim();
    } else if (line.startsWith('id:')) {
      id = line.slice('id:'.length).trim();
    } else if (line.startsWith('data:')) {
      data.push(line.slice('data:'.length).trim());
    }
  }

  if (data.length === 0 && id === null && event === 'message') {
    return null;
  }

  return { event, id, data: data.join('\n') };
}
