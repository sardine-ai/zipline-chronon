import { hsl } from 'd3';

export function getCssColorAsHex(cssVariable: string) {
	if (typeof window === 'undefined') return '';

	const hslValue = getComputedStyle(document.documentElement).getPropertyValue(cssVariable).trim();
	if (!hslValue) return '';

	const [h, s, l] = hslValue.split(' ').map((val) => parseFloat(val.replace('%', '')));
	return hsl(h, s / 100, l / 100).formatHex();
}

type ColorScaleOptions = {
	default?: string;
	foreground?: string;
};

export function createColorScale(
	name: string,
	options: ColorScaleOptions = {
		default: '300',
		foreground: '800'
	}
) {
	const shades = [50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 950];
	return {
		...(options.default && { DEFAULT: `hsl(var(--${name}-${options.default}) / <alpha-value>)` }),
		...(options.foreground && {
			foreground: `hsl(var(--${name}-${options.foreground}) / <alpha-value>)`
		}),
		...Object.fromEntries(
			shades.map((shade) => [shade, `hsl(var(--${name}-${shade}) / <alpha-value>)`])
		)
	};
}
