<script lang="ts">
	import { Status } from '$lib/types/codegen';
	import { getStripeBackground, getStatusClasses } from '$lib/job/status';

	let {
		status,
		isActive = false,
		includeHover = false
	} = $props<{
		status: Status | undefined;
		isActive?: boolean;
		includeHover?: boolean;
	}>();
</script>

<div
	class={`h-4 w-full rounded border transition-all ${getStatusClasses(status, isActive, includeHover)}`}
	style={getStripeBackground(status)}
></div>
