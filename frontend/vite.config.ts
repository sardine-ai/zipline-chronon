import { defineConfig } from 'vitest/config';
import { sveltekit } from '@sveltejs/kit/vite';
import tailwindcss from '@tailwindcss/vite';
import Icons from 'unplugin-icons/vite';
import { promises as fs } from 'node:fs';

export default defineConfig({
	plugins: [
		tailwindcss(),
		sveltekit(),
		Icons({
			compiler: 'svelte',
			customCollections: {
				'zipline-ai': {
					'chart-line': () => fs.readFile('./src/lib/icons/chart-line.svg', 'utf-8'),
					'chart-skew': () => fs.readFile('./src/lib/icons/chart-skew.svg', 'utf-8')
				}
			}
		})
	],
	test: {
		include: ['./src/**/*.{test,spec}.{js,ts}']
	},
	// this is not an ideal solution, as it increases dev server startup time
	// however, its the only thing that resolves build errors after switching git branches
	// https://github.com/vitejs/vite/discussions/17738#discussioncomment-10942635
	optimizeDeps: { force: true }
});
