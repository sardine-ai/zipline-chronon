export function isMacOS(): boolean {
	return typeof navigator !== 'undefined' && navigator.userAgent.toLowerCase().includes('mac');
}
