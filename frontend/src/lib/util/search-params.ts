import type { EncodeAndDecodeOptions } from 'sveltekit-search-params/sveltekit-search-params';

/** Get URLSearchParam values from `sveltekit-search-params` config.
 *  Mostly useful server-side (use `queryParameters()` client side)
 **/
export function getSearchParamValues<
	TConfig extends Record<string, EncodeAndDecodeOptions>,
	TKey extends keyof TConfig = keyof TConfig,
	TReturn = {
		[K in TKey]: TConfig[K] extends EncodeAndDecodeOptions<infer U> ? U : never;
	}
>(searchParams: URLSearchParams, paramsConfig: TConfig): TReturn {
	const paramEntries = Object.entries(paramsConfig).map(([paramName, paramConfig]) => {
		const value = searchParams.get(paramName);
		const decodedValue = paramConfig.decode(value) ?? paramConfig.defaultValue;
		return [paramName, decodedValue];
	});
	return Object.fromEntries(paramEntries) as TReturn;
}
