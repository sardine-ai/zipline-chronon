<script lang="ts">
	import { navigating } from '$app/state';
	import { expoOut } from 'svelte/easing';
	import { slide } from 'svelte/transition';
</script>

{#if navigating.to}
	<div
		class="fixed w-full top-0 right-0 left-0 h-[1px] z-50 bg-black dark:bg-white"
		in:slide={{ duration: 2000, axis: 'x', easing: expoOut }}
	></div>
{/if}
