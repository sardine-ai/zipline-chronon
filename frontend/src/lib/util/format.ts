import { format, PeriodType } from '@layerstack/utils';

/**
 * Formats a timestamp into a human-readable date string.
 * For example: Dec 31, 2022, 5:00:00 PM
 */
export function formatDate(value: Date | string | number | null | undefined): string {
	if (value == null || value === undefined) {
		return '';
	} else {
		const date = value instanceof Date ? value : new Date(value);
		return format(date, PeriodType.Custom, {
			custom: {
				month: 'short',
				day: 'numeric',
				year: 'numeric',
				hour: 'numeric',
				minute: '2-digit',
				second: '2-digit',
				hour12: true
			}
		});
	}
}

/**
 * Formats a numeric value, limiting decimal places to 4 if needed
 */
export function formatValue(value: string | number): string {
	if (typeof value === 'number') {
		// Only format to 4 decimals if the number has more decimal places
		const decimalPlaces = value.toString().split('.')[1]?.length || 0;
		return decimalPlaces > 4 ? value.toFixed(4) : value.toString();
	}
	return String(value);
}
