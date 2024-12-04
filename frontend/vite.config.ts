import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vitest/config';

export default defineConfig({
	plugins: [sveltekit()],
	test: {
		include: ['./src/**/*.{test,spec}.{js,ts}']
	},
	// this is not an ideal solution, as it increases dev server startup time
	// however, its the only thing that resolves build errors after switching git branches
	// https://github.com/vitejs/vite/discussions/17738#discussioncomment-10942635
	optimizeDeps: { force: true }
});
