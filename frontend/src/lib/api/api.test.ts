import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { get } from './api';
import { error } from '@sveltejs/kit';

// Mock the fetch function
const mockFetch = vi.fn();
global.fetch = mockFetch;

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

			const result = await get('test-path');

			expect(mockFetch).toHaveBeenCalledWith(`http://localhost:9000/api/v1/test-path`, {
				method: 'GET',
				headers: {}
			});
			expect(result).toEqual(mockResponse);
		});

		it('should return an empty object if the response is empty', async () => {
			mockFetch.mockResolvedValueOnce({
				ok: true,
				text: () => Promise.resolve('')
			});

			const result = await get('empty-path');

			expect(result).toEqual({});
		});

		it('should throw an error if the response is not ok', async () => {
			mockFetch.mockResolvedValueOnce({
				ok: false,
				status: 404
			});

			await get('error-path');

			expect(error).toHaveBeenCalledWith(404);
		});
	});
});
