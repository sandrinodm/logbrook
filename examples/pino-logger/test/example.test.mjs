import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

const exampleDirectory = fileURLToPath(new URL('../', import.meta.url));
const token = 'test-ingestion-token';

// Keep the receiver outside the Pino child: ending a worker transport can block
// its main thread while waiting for the HTTP requests to finish.
async function receiver(t, respond = () => 200) {
  const requests = [];
  const server = createServer(async (request, response) => {
    try {
      const chunks = [];
      for await (const chunk of request) {
        chunks.push(chunk);
      }

      const body = Buffer.concat(chunks);
      const batch = JSON.parse(body.toString('utf8'));
      const received = {
        batch,
        bodyBytes: body.length,
        path: request.url,
        method: request.method,
        headers: request.headers,
      };
      requests.push(received);

      const status = await respond(received, requests.length);
      response.writeHead(status, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ accepted: batch.length }));
    } catch (error) {
      response.writeHead(500);
      response.end(String(error));
    }
  });

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });

  t.after(async () => {
    server.closeAllConnections();

    await new Promise((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  });

  return { url: `http://127.0.0.1:${server.address().port}`, requests };
}

function runExample(t, script, url, overrides = {}, timeoutMs = 6000) {
  const env = {
    ...process.env,
    LOGBROOK_URL: url,
    LOGBROOK_INDEX: 'pino-demo',
    LOGBROOK_INGEST_TOKEN: token,
    BATCH_SIZE: '3',
    BATCH_INTERVAL_MS: '1000',
    MAX_BUFFER_SIZE: '10000',
    HTTP_TIMEOUT_MS: '500',
    MAX_RETRIES: '0',
    RETRY_DELAY_MS: '5',
    LOG_COUNT: '7',
    LOG_RATE: '1000',
    RUN_ID: 'test-run',
    ...overrides,
  };

  // Omit unset overrides entirely so child processes exercise missing settings.
  for (const [key, value] of Object.entries(env)) {
    if (value === undefined) {
      delete env[key];
    }
  }

  const child = spawn(process.execPath, Array.isArray(script) ? script : [script], {
    cwd: exampleDirectory,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout.setEncoding('utf8').on('data', (chunk) => {
    stdout += chunk;
  });
  child.stderr.setEncoding('utf8').on('data', (chunk) => {
    stderr += chunk;
  });

  let timedOut = false;
  const timeout = setTimeout(() => {
    timedOut = true;
    child.kill('SIGKILL');
  }, timeoutMs);

  const completion = new Promise((resolve, reject) => {
    child.once('error', (error) => {
      clearTimeout(timeout);
      reject(error);
    });

    // Wait for close so assertions include the last stdout and stderr chunks.
    child.once('close', (code, signal) => {
      clearTimeout(timeout);
      resolve({ code, signal, stdout, stderr, timedOut });
    });
  });

  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
    }
    await completion;
  });

  return { child, completion };
}

function assertSuccess(result) {
  assert.equal(result.timedOut, false, `Child timed out: ${result.stderr}`);
  assert.equal(result.signal, null, result.stderr);
  assert.equal(result.code, 0, result.stderr);
}

function assertFailure(result) {
  assert.equal(result.timedOut, false, `Child timed out: ${result.stderr}`);
  assert.equal(result.signal, null, result.stderr);
  assert.notEqual(result.code, 0, 'The example should report failure to its caller');
}

function records(requests) {
  return requests.flatMap((request) => request.batch);
}

test('basic sends Pino fields, child bindings, and a serialized error to a prefixed URL', async (t) => {
  const mock = await receiver(t);

  const { completion } = runExample(t, 'basic.mjs', `${mock.url}/gateway/`, { BATCH_SIZE: '2' });

  assertSuccess(await completion);
  assert.deepEqual(
    mock.requests.map(({ batch }) => batch.length),
    [2, 2],
  );

  for (const request of mock.requests) {
    assert.equal(request.path, '/gateway/indexes/pino-demo/logs/ingest');
    assert.equal(request.method, 'POST');
    assert.equal(request.headers.authorization, `Bearer ${token}`);
    assert.equal(request.headers['content-type'], 'application/json');
  }

  const received = records(mock.requests);

  assert.equal(received.length, 4);

  for (const record of received) {
    assert.equal(typeof record.level, 'number');
    assert.equal(Number.isInteger(record.time), true);
    assert.equal(typeof record.msg, 'string');
    assert.equal(typeof record.pid, 'number');
    assert.equal(typeof record.hostname, 'string');
  }

  assert.ok(
    received.some((record) => typeof record.requestId === 'string'),
    'Child request bindings are preserved',
  );

  const errorRecord = received.find((record) => record.err);

  assert.ok(errorRecord, 'The basic example includes an error');
  assert.equal(errorRecord.err.type, 'Error');
  assert.equal(typeof errorRecord.err.message, 'string');
  assert.match(errorRecord.err.stack, /Error:/);
});

test('bulk delivers full batches and the final partial batch in sequence', async (t) => {
  const mock = await receiver(t);

  const { completion } = runExample(t, 'bulk.mjs', mock.url);

  assertSuccess(await completion);
  assert.deepEqual(
    mock.requests.map(({ batch }) => batch.length),
    [3, 3, 1],
  );
  assert.deepEqual(
    records(mock.requests).map((record) => record.sequence),
    [0, 1, 2, 3, 4, 5, 6],
  );

  for (const record of records(mock.requests)) {
    assert.equal(record.runId, 'test-run');
    assert.equal(record.msg, 'bulk example run=test-run');
    assert.equal(record.level, 30);
  }
});

test('partial batches flush while a slow producer is still running', async (t) => {
  const mock = await receiver(t);

  const { completion } = runExample(t, 'bulk.mjs', mock.url, {
    LOG_COUNT: '3',
    LOG_RATE: '4',
    BATCH_SIZE: '250',
    BATCH_INTERVAL_MS: '30',
  });

  assertSuccess(await completion);
  assert.deepEqual(
    mock.requests.map(({ batch }) => batch.length),
    [1, 1, 1],
  );
  assert.deepEqual(
    records(mock.requests).map((record) => record.sequence),
    [0, 1, 2],
  );
});

test('a failed HTTP batch is retried before the next batch', async (t) => {
  const mock = await receiver(t, (_request, attempt) => (attempt === 1 ? 500 : 200));

  const { completion } = runExample(t, 'bulk.mjs', mock.url, {
    LOG_COUNT: '5',
    BATCH_SIZE: '4',
    MAX_RETRIES: '1',
  });

  assertSuccess(await completion);
  assert.deepEqual(
    mock.requests.map(({ batch }) => batch.map((record) => record.sequence)),
    [[0, 1, 2, 3], [0, 1, 2, 3], [4]],
  );
});

for (const status of [401, 413]) {
  test(`permanent HTTP ${status} makes shutdown fail`, async (t) => {
    const mock = await receiver(t, () => status);

    const { completion } = runExample(t, 'basic.mjs', mock.url);
    const result = await completion;

    assertFailure(result);
    assert.ok(mock.requests.length > 0);
    assert.match(result.stderr, new RegExp(`HTTP ${status}`));
  });
}

for (const [name, script, overrides] of [
  ['missing ingestion token', 'basic.mjs', { LOGBROOK_INGEST_TOKEN: undefined }],
  ['batch exceeding the server event limit', 'basic.mjs', { BATCH_SIZE: '1001' }],
  ['empty transport buffer', 'basic.mjs', { MAX_BUFFER_SIZE: '0' }],
  ['invalid producer rate', 'bulk.mjs', { LOG_RATE: '0' }],
]) {
  test(`${name} fails before sending HTTP requests`, async (t) => {
    const mock = await receiver(t);

    const { completion } = runExample(t, script, mock.url, overrides);

    assertFailure(await completion);
    assert.equal(mock.requests.length, 0);
  });
}

test('SIGTERM stops bulk production and drains records through normal shutdown', async (t) => {
  let child;
  let signalled = false;
  const mock = await receiver(t, async (_request, attempt) => {
    if (attempt === 1) {
      // Hold the active delivery while additional records accumulate, then stop
      // production. Shutdown must wait for this request and its queued records.
      await new Promise((resolve) => setTimeout(resolve, 50));
      signalled = child.kill('SIGTERM');
    }

    return 200;
  });

  const run = runExample(t, 'bulk.mjs', mock.url, {
    LOG_COUNT: '10000',
    LOG_RATE: '1000',
    BATCH_SIZE: '17',
  });
  child = run.child;

  assertSuccess(await run.completion);
  assert.equal(signalled, true);

  const received = records(mock.requests);

  assert.ok(received.length > 17, 'Queued records are delivered after the active request');
  assert.ok(received.length < 10000, 'The signal stops the producer early');
  assert.deepEqual(
    received.map((record) => record.sequence),
    Array.from({ length: received.length }, (_, index) => index),
  );
  assert.ok(mock.requests.every(({ batch }) => batch.length <= 17));
});

test(
  'shutdown delivers a slow backlog exceeding the worker end budget',
  { timeout: 20_000 },
  async (t) => {
    // Sequential slow responses keep delivery running beyond the worker end budget.
    const mock = await receiver(t, async () => {
      await new Promise((resolve) => setTimeout(resolve, 3500));

      return 200;
    });

    const started = performance.now();

    const { completion } = runExample(
      t,
      'bulk.mjs',
      mock.url,
      {
        LOG_COUNT: '3',
        BATCH_SIZE: '1',
        HTTP_TIMEOUT_MS: '5000',
      },
      18_000,
    );

    assertSuccess(await completion);
    assert.ok(performance.now() - started > 10_000);
    assert.deepEqual(
      records(mock.requests).map((record) => record.sequence),
      [0, 1, 2],
    );
  },
);

test('the default byte limit splits batches without losing records', async (t) => {
  const mock = await receiver(t);
  const script = [
    '--input-type=module',
    '-e',
    `import { createLogger } from './logger.mjs';
     const { logger, close } = await createLogger();
     for (let sequence = 0; sequence < 4; sequence++) {
       logger.info({ sequence, payload: 'x'.repeat(350_000) }, 'large test record');
     }
     await close();`,
  ];

  const { completion } = runExample(t, script, mock.url, { BATCH_SIZE: '250' });

  assertSuccess(await completion);
  assert.deepEqual(
    mock.requests.map(({ batch }) => batch.length),
    [2, 2],
  );
  assert.ok(mock.requests.every(({ bodyBytes }) => bodyBytes <= 1_048_576));
  assert.deepEqual(
    records(mock.requests).map((record) => record.sequence),
    [0, 1, 2, 3],
  );
});
