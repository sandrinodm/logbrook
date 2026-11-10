import { createServer } from 'node:net';

/**
 * Asks the operating system for a TCP port nobody is listening on, by binding port 0 and releasing
 * it. Another process could take the port before the caller uses it; on a test machine that is rare
 * enough to accept.
 */
export function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      server.close(() => {
        if (address !== null && typeof address === 'object') {
          resolve(address.port);
        } else {
          reject(new Error('Could not read the free port'));
        }
      });
    });
  });
}
