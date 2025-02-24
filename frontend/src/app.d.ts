export * from 'unplugin-icons/types/svelte';

import type { DateValue } from '$lib/components/charts/common';
import type { ComponentProps } from 'svelte';
import type { FeaturesLineChart } from '$lib/components/charts/FeaturesLineChart.svelte';
import type { CustomNode } from '$lib/types';
// See https://kit.svelte.dev/docs/types#app
// for information about these interfaces
declare global {
	namespace App {
		// interface Error {}
		// interface Locals {}
		// interface PageData {}
		interface PageState {
			selectedNode?: CustomNode | null;
			selectedSeriesPoint?: {
				series: NonNullable<ComponentProps<typeof FeaturesLineChart>['series']>[number];
				data: DateValue;
			} | null;
		}
		// interface Platform {}
	}
}

export {};
