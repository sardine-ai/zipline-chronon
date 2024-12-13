<script lang="ts">
	import { navigating } from '$app/stores';
	import { expoOut } from 'svelte/easing';
	import { slide } from 'svelte/transition';
</script>

{#if $navigating}
	<div
		class="fixed w-full top-0 right-0 left-0 h-[1px] z-50 bg-white"
		in:slide={{ duration: 2000, axis: 'x', easing: expoOut }}
	></div>
{/if}
