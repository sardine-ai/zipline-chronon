<script lang="ts">
	import { Dialog as DialogPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = DialogPrimitive.TitleProps;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<DialogPrimitive.Title
	class={cls('text-lg font-semibold leading-none tracking-tight', className)}
	{...$$restProps}
>
	<slot />
</DialogPrimitive.Title>
