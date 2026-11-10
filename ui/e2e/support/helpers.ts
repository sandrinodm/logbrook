import { request as httpRequest } from 'node:http';

/**
 * Rejects when `promise` does not settle within `millis`, naming `label`. Used so a stream that
 * never delivers (because something buffers it) fails the test instead of hanging it.
 */
export function withTimeout<T>(promise: Promise<T>, millis: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Timed out after ${millis} ms: ${label}`)),
      millis,
    );
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

/**
 * Waits for `promise` to settle and resolves with its rejection reason, so a test can assert on the
 * error with `toMatchObject` (the `e2e` runner has no `rejects`). A promise that resolves does not
 * pass silently: it yields `{ resolvedWith: value }`, which no error expectation matches.
 */
export async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    return { resolvedWith: await promise };
  } catch (error: unknown) {
    return error;
  }
}

/**
 * Polls `check` every 50 ms until it returns true, or fails after `millis`.
 */
export async function eventually(
  check: () => Promise<boolean>,
  millis: number,
  label: string,
): Promise<void> {
  const deadline = Date.now() + millis;
  while (Date.now() < deadline) {
    if (await check()) {
      return;
    }

    await new Promise((done) => setTimeout(done, 50));
  }

  throw new Error(`Condition not met within ${millis} ms: ${label}`);
}

/**
 * Runs every teardown step even when others fail, so one hung or failing step (a browser that will
 * not close) can never leave a server running. The steps run at the same time; if any rejects, the
 * returned promise rejects after all have settled. Steps that are `undefined` (a `beforeAll` that
 * failed before creating them) are skipped.
 */
export async function teardown(
  ...steps: ReadonlyArray<(() => Promise<unknown>) | undefined>
): Promise<void> {
  const results = await Promise.allSettled(steps.map(async (step) => step?.()));
  const failures = results.flatMap((result) =>
    result.status === 'rejected' ? [result.reason] : [],
  );
  if (failures.length > 0) {
    throw new AggregateError(failures, 'Teardown failed');
  }
}

/**
 * Sends `GET path` to `url` with an arbitrary `Host` header, which `fetch` does not allow. Resolves
 * with the status and the body.
 */
export function getWithHost(
  url: string,
  path: string,
  host: string,
): Promise<{ readonly status: number; readonly body: string }> {
  return new Promise((resolve, reject) => {
    const request = httpRequest(new URL(path, url), { headers: { host } }, (response) => {
      let body = '';
      response.setEncoding('utf8');
      response.on('data', (chunk: string) => {
        body += chunk;
      });
      response.on('end', () => resolve({ status: response.statusCode ?? 0, body }));
    });
    request.on('error', reject);
    request.end();
  });
}
