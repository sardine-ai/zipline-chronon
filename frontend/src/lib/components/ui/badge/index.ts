import { type VariantProps, tv } from 'tailwind-variants';

export { default as Badge } from './badge.svelte';
export const badgeVariants = tv({
	base: 'focus:ring-ring inline-flex select-none items-center rounded-md border border-neutral-500 px-2 py-0.5 text-xs transition-colors focus:outline-hidden focus:ring-2 focus:ring-offset-2',
	variants: {
		variant: {
			default:
				'bg-primary text-primary-foreground hover:bg-primary/80 border-transparent shadow-sm',
			secondary: 'bg-secondary text-secondary-foreground hover:bg-secondary/80 border-transparent',
			destructive:
				'bg-destructive text-destructive-foreground hover:bg-destructive/80 border-transparent shadow-sm',
			outline: 'text-foreground',
			key: '',
			'key-bg': 'border-transparent bg-neutral-400',
			success: 'bg-success-50 text-success-500 border-transparent' // todo use our style guide when it is created
		}
	},
	compoundVariants: [
		{
			variant: ['key', 'key-bg'],
			class: 'text-foreground p-0 h-[18px] w-[18px] justify-center rounded-sm'
		}
	],
	defaultVariants: {
		variant: 'default'
	}
});

export type Variant = VariantProps<typeof badgeVariants>['variant'];
