import type { DomainType } from 'layerchart/utils/scales.svelte';

export const shared = $state<{ xDomain: DomainType | undefined }>({ xDomain: null });

export function resetZoom() {
	shared.xDomain = null;
}

export function isZoomed() {
	return shared.xDomain != null;
}
