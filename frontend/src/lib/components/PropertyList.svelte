<script lang="ts">
	import { cls } from '@layerstack/tailwind';
	import TrueFalseBadge from './TrueFalseBadge.svelte';
	import JobStatusRect from '$src/routes/[conf]/[name]/offline/data/JobStatusRect.svelte';
	import { Status } from '../types/codegen';

	let {
		items,
		align = 'left'
	}: {
		items: Array<
			| {
					label?: string;
					value: string | number | boolean | object | null | undefined;
					type?: 'auto' | 'boolean' | 'object' | 'string' | 'status';
			  }
			| null
			| undefined
		>;
		align?: 'left' | 'right';
	} = $props();
</script>

<div class="border border-neutral-400 rounded-md text-[13px]">
	{#each items.filter((x) => !!x) as { label, value, type = 'auto' }}
		<div class="border-b last:border-b-0 border-neutral-400 flex whitespace-nowrap">
			{#if label}
				<span
					class={cls('text-muted-foreground px-3 py-2', align === 'left' && 'w-[200px] border-r')}
				>
					{label}
				</span>
			{/if}

			<span
				class={cls(
					'text-ellipsis overflow-hidden flex-1 px-3 py-2',
					align === 'right' && 'text-right'
				)}
			>
				{#if typeof value === 'boolean' || type === 'boolean'}
					<TrueFalseBadge value={value as boolean} />
				{:else if type === 'status'}
					<JobStatusRect status={value as Status} includeLabel />
				{:else if typeof value === 'object' || type === 'object'}
					{#each Object.entries(value ?? {}) as [key, v]}
						<div>
							<span class="text-muted-foreground">{key}:</span>
							{v}
						</div>
					{/each}
				{:else}
					{value}
				{/if}
			</span>
		</div>
	{/each}
</div>
