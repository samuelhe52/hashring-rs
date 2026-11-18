import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { fileURLToPath } from 'node:url'

export default defineConfig({
  root: fileURLToPath(new URL('./client', import.meta.url)),
  plugins: [vue()],
  // Windows 侧编辑 /mnt/d 文件不会触发 WSL 原生监听，开发时需要轮询。
  server: { watch: { usePolling: Boolean(process.env.WSL_DISTRO_NAME) } },
  build: { outDir: '../dist', emptyOutDir: true },
})
