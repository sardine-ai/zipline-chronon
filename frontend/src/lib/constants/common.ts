import { parseISO } from 'date-fns';

// Maps to  `Constants.magicNullDouble` on server - https://github.com/zipline-ai/chronon/blob/main/api/src/main/scala/ai/chronon/api/Constants.scala#L87
export const NULL_VALUE = -1234567890;

// Temporary demo date range for join drift data (see: ObservabilityDemoDataLoader)
export const DEMO_DATE_START = parseISO('2023-01-01');
// export const DEMO_DATE_END = parseISO('2023-03-01');
// export const DEMO_DATE_END = parseISO('2023-02-28T19:00-05:00');
export const DEMO_DATE_END = parseISO('2023-02-11');
