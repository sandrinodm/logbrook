import { createFileRoute } from '@tanstack/react-router';

import { serverConfig } from '../../../server/config.server.ts';
import { handleProxyRequest } from '../../../server/proxy.server.ts';
import { shutdownSignal } from '../../../server/shutdown.server.ts';

/**
 * Streaming proxy from the browser to Logbrook: `/api/lb/<logbrook path>` becomes
 * `<LOGBROOK_URL>/<logbrook path>` for the allowlisted `GET` routes, with the configured read
 * token. There is no authentication here: a reverse proxy in front of the app is responsible for
 * that. The handler is registered for every method (`ANY`) so that others get a `405` instead of
 * the app's HTML page; the proxy itself only forwards `GET`.
 */
export const Route = createFileRoute('/api/lb/$')({
  server: {
    handlers: {
      ANY: ({ request }) => {
        const { logbrookUrl, readToken } = serverConfig();
        return handleProxyRequest(request, {
          upstreamUrl: logbrookUrl,
          readToken,
          fetch,
          shutdownSignal,
        });
      },
    },
  },
});
