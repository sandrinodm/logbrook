import { applyDefaultHost } from './host-guard.server.ts';

// Registered as a Nitro polyfill (`unenv.polyfill` in `vite.config.ts`), which Nitro imports before
// its Node server reads `HOST`. See `applyDefaultHost`.
applyDefaultHost(process.env);
