import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Api } from './api';
import { error } from '@sveltejs/kit';

// Mock the fetch function
const mockFetch = vi.fn();
global.fetch = mockFetch;

const api = new Api({ fetch });

// Mock the error function from @sveltejs/kit
vi.mock('@sveltejs/kit', () => ({
	error: vi.fn()
}));

describe('API module', () => {
	beforeEach(() => {
		vi.resetAllMocks();
	});

	afterEach(() => {
		vi.clearAllMocks();
	});

	describe('get function', () => {
		it('should make a GET request and return parsed JSON data', async () => {
			const mockResponse = { data: 'test data' };
			mockFetch.mockResolvedValueOnce({
				ok: true,
				text: () => Promise.resolve(JSON.stringify(mockResponse))
			});

			const result = await api.getModelList();

			expect(mockFetch).toHaveBeenCalledWith(`/api/v1/conf/list?confType=MODEL`, {
				method: 'GET',
				headers: {
					'Content-Type': 'application/json'
				}
			});
			expect(result).toEqual(mockResponse);
		});

		it('should return an empty object if the response is empty', async () => {
			mockFetch.mockResolvedValueOnce({
				ok: true,
				text: () => Promise.resolve('')
			});

			const result = await api.getModelList();

			expect(result).toEqual({});
		});

		it('should throw an error if the response is not ok', async () => {
			mockFetch.mockResolvedValueOnce({
				ok: false,
				status: 404
			});

			await api.getModelList();

			expect(error).toHaveBeenCalledWith(404);
		});
	});
});
