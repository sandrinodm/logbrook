import { useQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';

import { IngestAndMetricsView } from '@/components/ingest-and-metrics-view';
import { PanelSkeleton } from '@/components/panel-skeleton';
import { accessQuery } from '@/lib/access-query';
import { deriveCapabilities } from '@/lib/logbrook/capabilities';
import { metricsQuery } from '@/lib/logbrook/metrics-query';
import { indexesQuery } from '@/lib/logbrook/queries';
import { useMetrics } from '@/lib/logbrook/use-metrics';
import { noop } from '@/lib/noop';

/**
 * How to send events, what the server's tokens can reach, and the server's metrics. Client-only,
 * because the metrics poll runs in the browser through the proxy.
 */
export const Route = createFileRoute('/_app/ingest')({
  ssr: false,
  pendingComponent: PanelSkeleton,
  loader: ({ context }) => {
    void context.queryClient.query(metricsQuery()).catch(noop);
  },
  component: IngestPage,
});

/**
 * Ingest and metrics view over the polled `/metrics` text and the index list of the layout.
 */
function IngestPage() {
  const access = useQuery(accessQuery());
  const indexes = useQuery(indexesQuery()).data ?? [];
  const metrics = useMetrics();

  return (
    <IngestAndMetricsView
      indexes={indexes}
      capabilities={deriveCapabilities(access.data ?? { canManageIndexes: false }, indexes)}
      metrics={metrics}
    />
  );
}
