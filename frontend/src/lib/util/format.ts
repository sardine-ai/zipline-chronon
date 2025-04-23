import { format } from '@layerstack/utils';

/**
 * Formats drift value to 4 decimal places
 */
export function formatDrift(value: number) {
	return format(value, 'decimal', { fractionDigits: 4 });
}
