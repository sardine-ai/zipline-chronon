<script lang="ts">
	import { Avatar as AvatarPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = AvatarPrimitive.ImageProps;

	let className: $$Props['class'] = undefined;
	export let src: $$Props['src'] = undefined;
	export let alt: $$Props['alt'] = undefined;
	export { className as class };
</script>

<AvatarPrimitive.Image
	{src}
	{alt}
	class={cls('aspect-square h-full w-full', className)}
	{...$$restProps}
/>
