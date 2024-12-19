import type { PlaywrightTestConfig } from '@playwright/test';

const config: PlaywrightTestConfig = {
	webServer: {
		command: 'pnpm build && pnpm preview',
		port: 4173
	},
	testDir: './src/test/integration',
	testMatch: /(.+\.)?(test|spec)\.[jt]s/
};

export default config;
