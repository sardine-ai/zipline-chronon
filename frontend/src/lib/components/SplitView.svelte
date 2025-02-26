<script lang="ts">
	import { quintOut } from 'svelte/easing';
	import IconXMark from '~icons/heroicons/x-mark';

	import { tweened } from 'svelte/motion';
	import type { Snippet } from 'svelte';

	let {
		sidebarOpen = $bindable(),
		main,
		sidebar
	}: { sidebarOpen: boolean; main: Snippet; sidebar: Snippet } = $props();
	let sidebarWidth = $state(30);

	function handleResize(event: MouseEvent) {
		const startX = event.clientX;
		const startWidth = sidebarWidth;

		function onMouseMove(e: MouseEvent) {
			const dx = e.clientX - startX;
			sidebarWidth = Math.max(20, Math.min(80, startWidth - (dx / window.innerWidth) * 100));
		}

		function onMouseUp() {
			document.removeEventListener('mousemove', onMouseMove);
			document.removeEventListener('mouseup', onMouseUp);
		}

		document.addEventListener('mousemove', onMouseMove);
		document.addEventListener('mouseup', onMouseUp);
	}

	const sidebarWidthTweened = tweened(0, {
		duration: 300,
		easing: quintOut
	});

	$effect(() => {
		if (sidebarOpen) {
			sidebarWidthTweened.set(sidebarWidth);
		} else {
			sidebarWidthTweened.set(0);
		}
	});
</script>

<div class="flex overflow-hidden">
	<div style="width: {100 - $sidebarWidthTweened}%;">
		{@render main()}
	</div>
	{#if sidebarOpen}
		<div class="relative bg-background border-l p-6" style="width: {$sidebarWidthTweened}%;">
			<div
				class="absolute left-0 top-0 bottom-0 w-[5px] cursor-ew-resize"
				onmousedown={handleResize}
				role="presentation"
			></div>
			<button
				class="ring-offset-background focus:ring-ring data-[state=open]:bg-secondary absolute right-4 top-4 rounded-sm opacity-70 transition-opacity hover:opacity-100 focus:outline-hidden focus:ring-2 focus:ring-offset-2 disabled:pointer-events-none"
				onclick={() => (sidebarOpen = false)}
			>
				<IconXMark class="text-sm" />
				<span class="sr-only">Close</span>
			</button>
			<div class="overflow-hidden">
				{@render sidebar()}
			</div>
		</div>
	{/if}
</div>
