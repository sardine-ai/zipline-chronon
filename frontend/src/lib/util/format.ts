/**
 * Formats a timestamp into a human-readable date string
 */
export function formatDate(value: string | number, includeTime: boolean = true): string {
	const options: Intl.DateTimeFormatOptions = {
		month: 'short',
		day: 'numeric',
		year: 'numeric'
	};

	if (includeTime) {
		Object.assign(options, {
			hour: 'numeric',
			minute: '2-digit',
			second: '2-digit',
			hour12: true
		});
	}

	return new Date(value).toLocaleString('en-US', options);
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
