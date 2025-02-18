import { fontFamily } from 'tailwindcss/defaultTheme';
import typography from '@tailwindcss/typography';
import { createColorScale } from './src/lib/util/colors';
/** @type {import('tailwindcss').Config} */
const config = {
	plugins: [typography],
	darkMode: ['class'],
	content: ['./src/**/*.{html,js,svelte,ts}', './node_modules/layerchart/**/*.{svelte,js}'],
	safelist: [
		'dark',
		{
			pattern: /^grid-cols-\d+$/ // Preserves all grid-cols-{n} classes
		},
		{
			// Generic pattern that will match any job status
			pattern: /^border-jobs-(.*)-active-border$/,
			variants: ['hover']
		},
		{
			pattern: /^border-jobs-(.*)-border$/
		},
		{
			pattern: /^bg-jobs-(.*)-bg$/
		}
	],
	theme: {
		container: {
			center: true,
			padding: '2rem',
			screens: {
				'2xl': '1400px'
			}
		},
		extend: {
			colors: {
				border: 'hsl(var(--border) / <alpha-value>)',
				input: 'hsl(var(--input) / <alpha-value>)',
				ring: 'hsl(var(--ring) / <alpha-value>)',
				background: 'hsl(var(--background) / <alpha-value>)',
				foreground: 'hsl(var(--foreground) / <alpha-value>)',
				// todo add default and foreground shades for neutral and warning
				neutral: createColorScale('neutral'),
				warning: createColorScale('warning'),
				primary: createColorScale('primary'),
				secondary: {
					DEFAULT: 'hsl(var(--secondary) / <alpha-value>)',
					foreground: 'hsl(var(--secondary-foreground) / <alpha-value>)'
				},
				destructive: {
					DEFAULT: 'hsl(var(--destructive) / <alpha-value>)',
					foreground: 'hsl(var(--destructive-foreground) / <alpha-value>)'
				},
				muted: {
					DEFAULT: 'hsl(var(--muted) / <alpha-value>)',
					foreground: 'hsl(var(--muted-foreground) / <alpha-value>)',
					'icon-neutral': 'hsl(var(--muted-icon-neutral) / <alpha-value>)',
					'icon-primary': 'hsl(var(--muted-icon-primary) / <alpha-value>)'
				},
				accent: {
					DEFAULT: 'hsl(var(--accent) / <alpha-value>)',
					foreground: 'hsl(var(--accent-foreground) / <alpha-value>)'
				},
				popover: {
					DEFAULT: 'hsl(var(--popover) / <alpha-value>)',
					foreground: 'hsl(var(--popover-foreground) / <alpha-value>)'
				},
				card: {
					DEFAULT: 'hsl(var(--card) / <alpha-value>)',
					foreground: 'hsl(var(--card-foreground) / <alpha-value>)'
				},
				jobs: {
					WAITING_FOR_UPSTREAM: {
						bg: 'hsl(var(--job-WAITING_FOR_UPSTREAM-bg) / <alpha-value>)',
						border: 'hsl(var(--job-WAITING_FOR_UPSTREAM-border) / <alpha-value>)',
						'active-border': 'hsl(var(--job-WAITING_FOR_UPSTREAM-active-border) / <alpha-value>)'
					},
					RUNNING: {
						bg: 'hsl(var(--job-RUNNING-bg) / <alpha-value>)',
						border: 'hsl(var(--job-RUNNING-border) / <alpha-value>)',
						'active-border': 'hsl(var(--job-RUNNING-active-border) / <alpha-value>)'
					},
					FAILED: {
						bg: 'hsl(var(--job-FAILED-bg) / <alpha-value>)',
						border: 'hsl(var(--job-FAILED-border) / <alpha-value>)',
						'active-border': 'hsl(var(--job-FAILED-active-border) / <alpha-value>)'
					},
					SUCCESS: {
						bg: 'hsl(var(--job-SUCCESS-bg) / <alpha-value>)',
						border: 'hsl(var(--job-SUCCESS-border) / <alpha-value>)',
						'active-border': 'hsl(var(--job-SUCCESS-active-border) / <alpha-value>)'
					},
					QUEUED: {
						bg: 'hsl(var(--job-QUEUED-bg) / <alpha-value>)',
						border: 'hsl(var(--job-QUEUED-border) / <alpha-value>)',
						'active-border': 'hsl(var(--job-QUEUED-active-border) / <alpha-value>)'
					},
					UPSTREAM_FAILED: {
						bg: 'hsl(var(--job-UPSTREAM_FAILED-bg) / <alpha-value>)',
						border: 'hsl(var(--job-UPSTREAM_FAILED-border) / <alpha-value>)',
						'active-border': 'hsl(var(--job-UPSTREAM_FAILED-active-border) / <alpha-value>)'
					},
					UPSTREAM_MISSING: {
						bg: 'hsl(var(--job-UPSTREAM_MISSING-bg) / <alpha-value>)',
						border: 'hsl(var(--job-UPSTREAM_MISSING-border) / <alpha-value>)',
						'active-border': 'hsl(var(--job-UPSTREAM_MISSING-active-border) / <alpha-value>)'
					},
					WAITING_FOR_RESOURCES: {
						bg: 'hsl(var(--job-WAITING_FOR_RESOURCES-bg) / <alpha-value>)',
						border: 'hsl(var(--job-WAITING_FOR_RESOURCES-border) / <alpha-value>)',
						'active-border': 'hsl(var(--job-WAITING_FOR_RESOURCES-active-border) / <alpha-value>)'
					}
				},
				// Additional LayerChart colors
				surface: {
					content: 'hsl(var(--card-foreground) / <alpha-value>)',
					100: 'hsl(var(--background) / <alpha-value>)',
					200: 'hsl(var(--muted) / <alpha-value>)',
					// not sure what color maps here (should be darker than 200).  Could add a new color to `app.css`
					300: 'hsl(var(--background) / <alpha-value>)'
				}
			},
			borderRadius: {
				lg: 'var(--radius)',
				md: 'calc(var(--radius) - 2px)',
				sm: 'calc(var(--radius) - 4px)'
			},
			fontFamily: {
				sans: ['Geist Mono', 'Geist', 'Inter', ...fontFamily.sans]
			},
			fontSize: {
				'3xl-medium': [
					'3.625rem', // 58px
					{
						lineHeight: '3.75rem', // 60px
						letterSpacing: '-0.125rem', // -2px
						fontWeight: '500'
					}
				],
				'2xl-medium': [
					'2.25rem', // 36px
					{
						lineHeight: '2.75rem', // 44px
						letterSpacing: '-0.0625rem', // -1px
						fontWeight: '500'
					}
				],
				'xl-medium': [
					'1.25rem', // 20px
					{
						lineHeight: '1.75rem', // 28px
						letterSpacing: '0rem', // 0px
						fontWeight: '500'
					}
				],
				'large-medium': [
					'1rem', // 16px
					{
						lineHeight: '1.5rem', // 24px
						letterSpacing: '0rem',
						fontWeight: '500'
					}
				],
				'regular-medium': [
					'0.875rem', // 14px
					{
						lineHeight: '1.25rem', // 20px
						letterSpacing: '0rem',
						fontWeight: '500'
					}
				],
				regular: [
					'0.875rem', // 14px
					{
						lineHeight: '1.25rem', // 20px
						letterSpacing: '0rem',
						fontWeight: '400'
					}
				],
				'regular-mono': [], // todo
				small: [
					'0.8125rem', // 13px
					{
						lineHeight: '1rem', // 16px
						letterSpacing: '0rem',
						fontWeight: '400'
					}
				],
				'xs-medium': [
					'0.75rem', // 12px
					{
						lineHeight: '0.875rem', // 14px
						letterSpacing: '0rem',
						fontWeight: '500'
					}
				],
				xs: [
					'0.75rem', // 12px
					{
						lineHeight: '0.875rem', // 14px
						letterSpacing: '0rem',
						fontWeight: '400'
					}
				],
				'xs-mono': [] // todo
			},
			animation: {
				'dashoffset-0.5x': 'animated-dash 2s linear infinite',
				'dashoffset-1x': 'animated-dash 1s linear infinite',
				'dashoffset-2x': 'animated-dash 0.5s linear infinite',
				'dashoffset-3x': 'animated-dash 0.25s linear infinite',
				'dashoffset-4x': 'animated-dash 0.125s linear infinite',
				'dashoffset-5x': 'animated-dash 0.0625s linear infinite'
			},
			keyframes: {
				'animated-dash': {
					'100%': { strokeDashoffset: 0 }
				}
			}
		}
	}
};

export default config;
