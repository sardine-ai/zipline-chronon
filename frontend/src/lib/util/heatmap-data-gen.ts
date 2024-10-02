// Function to generate X-axis labels based on count (e.g., T1, T2, ..., Tn)
export function generateXAxis(count: number): string[] {
	return Array.from({ length: count }, (_, i) => `T${i + 1}`);
}

// Function to generate Y-axis values based on count (e.g., Feature 1, Feature 2, ..., Feature n)
export function generateYAxis(count: number): string[] {
	return Array.from({ length: count }, (_, i) => `Feature ${i + 1}`);
}

type DataType = 'normal' | 'anomalous' | 'slow-drift';

interface HeatmapConfig {
	type: DataType;
	affectedRows?: number[];
}

// Function to generate heatmap data (random values between specified range)
export function generateHeatmapData(xAxis: string[], yAxis: string[], config: HeatmapConfig) {
	const data = [];

	for (let i = 0; i < yAxis.length; i++) {
		const rowType = config.affectedRows?.includes(i) ? config.type : 'normal';

		for (let j = 0; j < xAxis.length; j++) {
			let value: number;

			switch (rowType) {
				case 'normal': {
					value = Math.random() * 0.2;
					break;
				}
				case 'anomalous': {
					value = Math.random() < 0.9 ? Math.random() * 0.2 : 0.8 + Math.random() * 0.2;
					break;
				}
				case 'slow-drift': {
					const progress = j / (xAxis.length - 1);
					value = 0.2 + 0.6 * progress + Math.random() * 0.2;
					break;
				}
			}

			data.push([j, i, value]);
		}
	}

	return data;
}
