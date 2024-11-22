import Root from './table.svelte';
import Body from './table-body.svelte';
import Caption from './table-caption.svelte';
import Cell from './table-cell.svelte';
import Footer from './table-footer.svelte';
import Head from './table-head.svelte';
import Header from './table-header.svelte';
import Row from './table-row.svelte';
import { type VariantProps, tv } from 'tailwind-variants';

const tableVariants = tv({
	base: 'w-full caption-bottom',
	variants: {
		density: {
			compact: 'text-small [&_td]:py-[0.45rem] [&_th]:py-[0.45rem]',
			default: 'text-regular'
		}
	},
	defaultVariants: {
		density: 'default'
	}
});

type Density = VariantProps<typeof tableVariants>['density'];

type Props = {
	density?: Density;
} & HTMLTableElement;

export {
	Root,
	Body,
	Caption,
	Cell,
	Footer,
	Head,
	Header,
	Row,
	//
	Root as Table,
	Body as TableBody,
	Caption as TableCaption,
	Cell as TableCell,
	Footer as TableFooter,
	Head as TableHead,
	Header as TableHeader,
	Row as TableRow,
	tableVariants,
	type Props
};
