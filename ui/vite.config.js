import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  // Only used by `npm run dev`. The production build is served by Spring itself.
  server: { proxy: { '/jobs': 'http://localhost:8080' } },
});
