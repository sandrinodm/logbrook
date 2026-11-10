import { createFileRoute } from '@tanstack/react-router';

/**
 * Liveness probe for the container health check: answers `200` whenever the process is up. It does
 * not call Logbrook, so a Logbrook that is down or a token it rejects never marks the UI unhealthy;
 * those show in the pages and in the boot log.
 */
export const Route = createFileRoute('/healthz')({
  server: {
    handlers: {
      GET: () => new Response('ok', { headers: { 'cache-control': 'no-store' } }),
    },
  },
});
