import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    // The repo lives on /mnt/c (Windows drive under WSL), where file-change events
    // are not reliably delivered — without polling, edits are never picked up by HMR.
    watch: { usePolling: true, interval: 300 },
    proxy: {
      '/chat': 'http://localhost:8000',
      '/nodes': 'http://localhost:8000',
      '/jobs': 'http://localhost:8000',
    },
  },
})
