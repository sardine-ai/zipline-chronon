<script lang="ts">
	import type { SvelteHTMLElements } from 'svelte/elements';
	import { transformContext } from 'layerchart';

	import IconArrowUturnLeft from '~icons/heroicons/arrow-uturn-left';
	import IconMagnifyingGlassPlus from '~icons/heroicons/magnifying-glass-plus';
	import IconMagnifyingGlassMinus from '~icons/heroicons/magnifying-glass-minus';

	import { Button } from '$lib/components/ui/button';
	import { Tooltip, TooltipContent, TooltipTrigger } from '$lib/components/ui/tooltip';
	import { cn } from '$lib/utils';

	type Placement =
		| 'top-left'
		| 'top'
		| 'top-right'
		| 'left'
		| 'center'
		| 'right'
		| 'bottom-left'
		| 'bottom'
		| 'bottom-right';

	type Actions = 'zoomIn' | 'zoomOut' | 'center' | 'reset' | 'scrollMode';
	interface Props {
		placement?: Placement;
		orientation?: 'horizontal' | 'vertical';
		size?: SvelteHTMLElements['svg']['width'];
		show?: Actions[];
		class?: string;
	}

	let {
		placement = 'top-right',
		orientation = 'vertical',
		size = '16',
		show = ['zoomIn', 'zoomOut', 'center', 'reset', 'scrollMode'],
		...restProps
	}: Props = $props();

	const transform = transformContext();
</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<div
	class={cn(
		'bg-surface-100/50 border rounded-full m-1 backdrop-blur-sm z-10 flex p-1',
		orientation === 'vertical' && 'flex-col',
		{
			'top-left': 'absolute top-0 left-0',
			top: 'absolute top-0 left-1/2 -translate-x-1/2',
			'top-right': 'absolute top-0 right-0',
			left: 'absolute top-1/2 left-0 -translate-y-1/2',
			center: 'absolute top-1/2 left-1/2 -translate-x-1/2  -translate-y-1/2',
			right: 'absolute top-1/2 right-0 -translate-y-1/2',
			'bottom-left': 'absolute bottom-0 left-0',
			bottom: 'absolute bottom-0 left-1/2 -translate-x-1/2',
			'bottom-right': 'absolute bottom-0 right-0'
		}[placement],
		restProps.class
	)}
	ondblclick={(e) => {
		// Stop from propagating to TransformContext
		e.stopPropagation();
	}}
>
	{#if show.includes('zoomIn')}
		<Tooltip>
			<TooltipTrigger>
				<Button
					on:click={() => transform.zoomIn()}
					size="icon"
					variant="ghost"
					class="rounded-full"
				>
					<IconMagnifyingGlassPlus width={size} height={size} />
				</Button>
			</TooltipTrigger>
			<TooltipContent side="left">Zoom in</TooltipContent>
		</Tooltip>
	{/if}

	{#if show.includes('zoomOut')}
		<Tooltip>
			<TooltipTrigger>
				<Button
					on:click={() => transform.zoomOut()}
					size="icon"
					variant="ghost"
					class="rounded-full"
				>
					<IconMagnifyingGlassMinus width={size} height={size} />
				</Button>
			</TooltipTrigger>
			<TooltipContent side="left">Zoom out</TooltipContent>
		</Tooltip>
	{/if}

	{#if show.includes('reset')}
		<Tooltip>
			<TooltipTrigger>
				<Button on:click={() => transform.reset()} size="icon" variant="ghost" class="rounded-full">
					<IconArrowUturnLeft width={size} height={size} />
				</Button>
			</TooltipTrigger>
			<TooltipContent side="left">Reset</TooltipContent>
		</Tooltip>
	{/if}
</div>
