import type { BarChart, LineChart, PieChart } from 'layerchart';
import type { ComponentProps } from 'svelte';

export type DateValue = { date: Date; value: number };

export const colors = [
	'#E5174B',
	'#E54D4A',
	'#E17545',
	'#E3994C',
	'#DFAF4F',
	'#87BE52',
	'#53B167',
	'#4DA67D',
	'#4EA797',
	'#4491CE',
	'#4592CC',
	'#4172D2',
	'#5B5AD1',
	'#785AD4',
	'#9055D5',
	'#BF50D3',
	'#CB5587'
];

export const xAxisProps = {
	placement: 'bottom' as const,
	classes: {
		tickLabel: 'fill-neutral-700 text-xs'
	}
};

export const yAxisProps = {
	placement: 'left' as const,
	classes: {
		tickLabel: 'fill-neutral-700 text-xs'
	}
};

export const tooltipProps = {
	root: {
		variant: 'none',
		class: 'text-small bg-neutral-200 border border-neutral-400 rounded-md shadow-lg',
		motion: false
	},
	header: {
		class: 'text-neutral-700 bg-neutral-300 px-3 py-1'
	},
	list: {
		class: 'gap-4 px-3 py-2'
	},
	item: {
		valueAlign: 'right',
		classes: {
			label: 'text-neutral-800'
		}
	}
} satisfies NonNullable<ComponentProps<typeof LineChart>['props']>['tooltip'];

export const highlightProps = {
	motion: false,
	points: {
		r: 8,
		strokeWidth: 4
	}
} satisfies NonNullable<ComponentProps<typeof LineChart>['props']>['highlight'];

export const barChartProps = {
	props: {
		tooltip: tooltipProps
	}
} satisfies NonNullable<ComponentProps<typeof BarChart>>;

export const lineChartProps = {
	grid: {
		x: { class: 'stroke-neutral-500/30' },
		y: { class: 'stroke-neutral-500/30' }
	},
	props: {
		tooltip: tooltipProps,
		highlight: highlightProps,
		xAxis: xAxisProps,
		yAxis: yAxisProps
	}
} satisfies NonNullable<ComponentProps<typeof LineChart>>;

export const pieChartProps = {
	props: {
		tooltip: tooltipProps
	}
} satisfies NonNullable<ComponentProps<PieChart<unknown>>>;
