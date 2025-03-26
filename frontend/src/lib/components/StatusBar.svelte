<script lang="ts">
	import { Status } from '$lib/types/codegen';
	import { cls } from '@layerstack/tailwind';

	let { status } = $props<{
		status: Status | undefined;
	}>();
</script>

<div class={cls(`h-4 w-full rounded border transition-all`, Status[status].toLowerCase())}></div>

<style>
	.running,
	.waiting_for_resources {
		background: hsl(250 40% 60%);
		border: 1px solid hsl(250 38% 52%);

		&:hover {
			border: 1px solid hsl(250 80% 30%);
		}

		:global(html.dark &) {
			background: hsl(250 30% 24%);
			border: 1px solid hsl(250 30% 40%);

			&:hover {
				border: 1px solid hsl(250 48% 62%);
			}
		}
	}
	.waiting_for_resources {
		background: repeating-linear-gradient(
			135deg,
			hsl(250 40% 60%) 0 10px,
			hsl(250 40% 60% / 30%) 0 20px
		);
		:global(html.dark &) {
			background: repeating-linear-gradient(
				135deg,
				hsl(250 30% 24%) 0 10px,
				hsl(250 30% 24% / 30%) 0 20px
			);
		}
	}

	.waiting_for_upstream {
		background: hsl(210 40% 58%);
		border: 1px solid hsl(210 38% 50%);

		&:hover {
			border: 1px solid hsl(210 80% 30%);
		}

		:global(html.dark &) {
			background: hsl(210 30% 20%);
			border: 1px solid hsl(210 30% 34%);

			&:hover {
				border: 1px solid hsl(210 46% 56%);
			}
		}
	}

	.failed,
	.upstream_failed {
		background: hsl(10 35% 55%);
		border: 1px solid hsl(10 32% 48%);

		&:hover {
			border: 1px solid hsl(10 80% 30%);
		}

		:global(html.dark &) {
			background: hsl(10 24% 20%);
			border: 1px solid hsl(10 30% 34%);

			&:hover {
				border: 1px solid hsl(10 46% 56%);
			}
		}
	}
	.upstream_failed {
		background: repeating-linear-gradient(
			135deg,
			hsl(10 35% 55%) 0 10px,
			hsl(10 35% 55% / 30%) 0 20px
		);

		:global(html.dark &) {
			background: repeating-linear-gradient(
				135deg,
				hsl(10 24% 20%) 0 10px,
				hsl(10 24% 20%/ 30%) 0 20px
			);
		}
	}

	.success {
		background: hsl(160 45% 52%);
		border: 1px solid hsl(160 42% 45%);

		&:hover {
			border: 1px solid hsl(160 80% 30%);
		}

		:global(html.dark &) {
			background: hsl(160 40% 17%);
			border: 1px solid hsl(160 40% 28%);

			&:hover {
				border: 1px solid hsl(160 32% 47%);
			}
		}
	}

	.queued {
		background: hsl(0 0% 52%);
		border: 1px solid hsl(0 0% 46%);

		&:hover {
			border: 1px solid hsl(0 0% 20%);
		}

		:global(html.dark &) {
			background: hsl(0 0% 16%);
			border: 1px solid hsl(0 0% 24%);

			&:hover {
				border: 1px solid hsl(0 0% 40%);
			}
		}
	}

	.upstream_missing {
		background: hsl(38 40% 58%);
		border: 1px solid hsl(38 38% 50%);

		&:hover {
			border: 1px solid hsl(38 80% 30%);
		}

		:global(html.dark &) {
			background: hsl(38 30% 20%);
			border: 1px solid hsl(38 30% 34%);

			&:hover {
				border: 1px solid hsl(38 46% 56%);
			}
		}
	}
</style>
