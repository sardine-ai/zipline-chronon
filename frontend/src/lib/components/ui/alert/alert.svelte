<script lang="ts">
	import type { HTMLAttributes } from 'svelte/elements';
	import { type Variant, alertVariants } from './index.js';
	import { cls } from '@layerstack/tailwind';

	type $$Props = HTMLAttributes<HTMLDivElement> & {
		variant?: Variant;
	};

	let className: $$Props['class'] = undefined;
	export let variant: $$Props['variant'] = 'default';
	export { className as class };
</script>

<div class={cls(alertVariants({ variant }), className)} {...$$restProps} role="alert">
	<slot />
</div>
