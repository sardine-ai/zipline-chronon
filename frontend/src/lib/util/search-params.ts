import type { EncodeAndDecodeOptions } from 'sveltekit-search-params/sveltekit-search-params';

/** Get URLSearchParam values from `sveltekit-search-params` config.
 *  Mostly useful server-side (use `queryParameters()` client side)
 **/
export function getSearchParamValues(
	searchParams: URLSearchParams,
	paramsConfig: Record<string, EncodeAndDecodeOptions>
) {
	const paramEntries = Object.entries(paramsConfig).map(([paramName, paramConfig]) => {
		const value = searchParams.get(paramName);
		let decodedValue = paramConfig.decode(value) ?? paramConfig.defaultValue;
		return [paramName, decodedValue];
	});
	return Object.fromEntries(paramEntries);
}
