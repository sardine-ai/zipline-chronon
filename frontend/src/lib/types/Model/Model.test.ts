import { describe, it, expect } from 'vitest';
import * as api from '$lib/api/api';
import type {
	ModelsResponse,
	TimeSeriesResponse,
	Model,
	JoinTimeSeriesResponse
} from '$lib/types/Model/Model';

describe('Model types', () => {
	it('should match ModelsResponse type', async () => {
		const result = (await api.getModels()) as ModelsResponse;

		const expectedKeys = ['offset', 'items'];
		expect(Object.keys(result)).toEqual(expect.arrayContaining(expectedKeys));

		// Log a warning if there are additional fields
		const additionalKeys = Object.keys(result).filter((key) => !expectedKeys.includes(key));
		if (additionalKeys.length > 0) {
			console.warn(`Additional fields found in ModelsResponse: ${additionalKeys.join(', ')}`);
		}

		expect(Array.isArray(result.items)).toBe(true);

		if (result.items.length > 0) {
			const model = result.items[0];
			const expectedModelKeys: (keyof Model)[] = [
				'name',
				'online',
				'production',
				'team',
				'modelType',
				'join'
			];
			expect(Object.keys(model)).toEqual(expect.arrayContaining(expectedModelKeys));

			// Log a warning if there are additional fields
			const additionalModelKeys = Object.keys(model).filter(
				(key) => !expectedModelKeys.includes(key as keyof Model)
			);
			if (additionalModelKeys.length > 0) {
				console.warn(`Additional fields found in Model: ${additionalModelKeys.join(', ')}`);
			}
		}
	});

	it('should match TimeSeriesResponse type', async () => {
		const result = (await api.getModels()) as ModelsResponse;
		expect(result.items.length).toBeGreaterThan(0);

		const modelName = result.items[0].name;
		const timeseriesResult = (await api.getModelTimeseries(
			modelName,
			1725926400000,
			1726106400000
		)) as TimeSeriesResponse;

		const expectedKeys = ['id', 'items'];
		expect(Object.keys(timeseriesResult)).toEqual(expect.arrayContaining(expectedKeys));

		// Log a warning if there are additional fields
		const additionalKeys = Object.keys(timeseriesResult).filter(
			(key) => !expectedKeys.includes(key)
		);
		if (additionalKeys.length > 0) {
			console.warn(`Additional fields found in TimeSeriesResponse: ${additionalKeys.join(', ')}`);
		}

		expect(Array.isArray(timeseriesResult.items)).toBe(true);

		if (timeseriesResult.items.length > 0) {
			const item = timeseriesResult.items[0];
			const expectedItemKeys = ['value', 'ts', 'label'];
			expect(Object.keys(item)).toEqual(expect.arrayContaining(expectedItemKeys));

			// Log a warning if there are additional fields
			const additionalItemKeys = Object.keys(item).filter((key) => !expectedItemKeys.includes(key));
			if (additionalItemKeys.length > 0) {
				console.warn(
					`Additional fields found in TimeSeriesResponse item: ${additionalItemKeys.join(', ')}`
				);
			}
		}
	});

	it('should match ModelsResponse type for search results', async () => {
		const searchTerm = 'risk.transaction_model.v1';
		const limit = 5;
		const result = (await api.search(searchTerm, limit)) as ModelsResponse;

		const expectedKeys = ['offset', 'items'];
		expect(Object.keys(result)).toEqual(expect.arrayContaining(expectedKeys));

		// Log a warning if there are additional fields
		const additionalKeys = Object.keys(result).filter((key) => !expectedKeys.includes(key));
		if (additionalKeys.length > 0) {
			console.warn(
				`Additional fields found in search ModelsResponse: ${additionalKeys.join(', ')}`
			);
		}

		expect(Array.isArray(result.items)).toBe(true);
		expect(result.items.length).toBeLessThanOrEqual(limit);

		if (result.items.length > 0) {
			const model = result.items[0];
			const expectedModelKeys: (keyof Model)[] = [
				'name',
				'online',
				'production',
				'team',
				'modelType',
				'join'
			];
			expect(Object.keys(model)).toEqual(expect.arrayContaining(expectedModelKeys));

			// Log a warning if there are additional fields
			const additionalModelKeys = Object.keys(model).filter(
				(key) => !expectedModelKeys.includes(key as keyof Model)
			);
			if (additionalModelKeys.length > 0) {
				console.warn(`Additional fields found in search Model: ${additionalModelKeys.join(', ')}`);
			}

			// Check if the search term is included in the model name
			expect(model.name.toLowerCase()).toContain(searchTerm.toLowerCase());
		}
	});

	it('should match JoinTimeSeriesResponse type', async () => {
		const result = (await api.getModels()) as ModelsResponse;
		expect(result.items.length).toBeGreaterThan(0);

		const modelName = result.items[0].name;
		const joinResult = (await api.getJoinTimeseries(
			modelName,
			1725926400000,
			1726106400000
		)) as JoinTimeSeriesResponse;

		const expectedKeys = ['name', 'items'];
		expect(Object.keys(joinResult)).toEqual(expect.arrayContaining(expectedKeys));

		// Log a warning if there are additional fields
		const additionalKeys = Object.keys(joinResult).filter((key) => !expectedKeys.includes(key));
		if (additionalKeys.length > 0) {
			console.warn(
				`Additional fields found in JoinTimeSeriesResponse: ${additionalKeys.join(', ')}`
			);
		}

		expect(Array.isArray(joinResult.items)).toBe(true);

		if (joinResult.items.length > 0) {
			const item = joinResult.items[0];
			const expectedItemKeys = ['name', 'items'];
			expect(Object.keys(item)).toEqual(expect.arrayContaining(expectedItemKeys));

			// Log a warning if there are additional fields
			const additionalItemKeys = Object.keys(item).filter((key) => !expectedItemKeys.includes(key));
			if (additionalItemKeys.length > 0) {
				console.warn(
					`Additional fields found in JoinTimeSeriesResponse item: ${additionalItemKeys.join(', ')}`
				);
			}

			if (item.items.length > 0) {
				const subItem = item.items[0];
				const expectedSubItemKeys = ['feature', 'points'];
				expect(Object.keys(subItem)).toEqual(expect.arrayContaining(expectedSubItemKeys));

				// Log a warning if there are additional fields
				const additionalSubItemKeys = Object.keys(subItem).filter(
					(key) => !expectedSubItemKeys.includes(key)
				);
				if (additionalSubItemKeys.length > 0) {
					console.warn(
						`Additional fields found in JoinTimeSeriesResponse sub-item: ${additionalSubItemKeys.join(', ')}`
					);
				}

				expect(Array.isArray(subItem.points)).toBe(true);

				if (subItem.points.length > 0) {
					const point = subItem.points[0];
					const expectedPointKeys = ['value', 'ts', 'label'];
					expect(Object.keys(point)).toEqual(expect.arrayContaining(expectedPointKeys));

					// Log a warning if there are additional fields
					const additionalPointKeys = Object.keys(point).filter(
						(key) => !expectedPointKeys.includes(key)
					);
					if (additionalPointKeys.length > 0) {
						console.warn(
							`Additional fields found in JoinTimeSeriesResponse point: ${additionalPointKeys.join(', ')}`
						);
					}
				}
			}
		}
	});
});
