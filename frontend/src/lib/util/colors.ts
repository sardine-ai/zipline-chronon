import { hsl } from 'd3';

export function getCssColorAsHex(cssVariable: string) {
	if (typeof window === 'undefined') return '';

	const hslValue = getComputedStyle(document.documentElement).getPropertyValue(cssVariable).trim();
	if (!hslValue) return '';

	const [h, s, l] = hslValue.split(' ').map((val) => parseFloat(val.replace('%', '')));
	return hsl(h, s / 100, l / 100).formatHex();
}
