import type { EncodeAndDecodeOptions } from 'sveltekit-search-params/sveltekit-search-params';
import { getSearchParamValues } from './search-params';

export const DEFAULT_OFFSET = 15; // days

export function getOffsetParamsConfig() {
	return {
		offset: {
			encode: (value) => value.toString(),
			decode: (value) => (value ? Number(value) : null),
			defaultValue: DEFAULT_OFFSET
		} satisfies EncodeAndDecodeOptions<number>
	};
}

/** Get offset from query string (or default) in days */
export function getOffset(searchParams: URLSearchParams) {
	const paramsConfig = getOffsetParamsConfig();
	return getSearchParamValues(searchParams, paramsConfig).offset;
}
