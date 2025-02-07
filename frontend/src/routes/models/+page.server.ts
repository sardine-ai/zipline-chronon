import type { PageServerLoad } from './$types';
import { loadConfList } from '$lib/server/conf-loader';

export const load: PageServerLoad = loadConfList;
