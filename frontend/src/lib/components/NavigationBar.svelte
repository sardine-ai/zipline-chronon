<script lang="ts">
	import { Avatar, AvatarFallback, AvatarImage } from '$lib/components/ui/avatar';
	import { Button } from '$lib/components/ui/button';
	import { page } from '$app/stores';
	import {
		CommandDialog,
		CommandInput,
		CommandList,
		CommandGroup,
		CommandItem,
		CommandEmpty
	} from '$lib/components/ui/command/';
	import { Api } from '$lib/api/api';
	import type { Model } from '$lib/types/Model/Model';
	import debounce from 'lodash/debounce';
	import { onDestroy, onMount } from 'svelte';
	import {
		DropdownMenu,
		DropdownMenuTrigger,
		DropdownMenuContent,
		DropdownMenuItem
	} from '$lib/components/ui/dropdown-menu/';
	import { goto } from '$app/navigation';
	import { isMacOS } from '$lib/util/browser';
	import { Badge } from '$lib/components/ui/badge';
	import { getEntity, type Entity } from '$lib/types/Entity/Entity';

	import IconArrowsUpDown from '~icons/heroicons/arrows-up-down-16-solid';
	import IconAdjustmentsHorizontal from '~icons/heroicons/adjustments-horizontal-16-solid';
	import IconChevronDown from '~icons/heroicons/chevron-down';
	import IconDocumentText from '~icons/heroicons/document-text-16-solid';
	import IconExclamationTriangle from '~icons/heroicons/exclamation-triangle-16-solid';
	import IconMagnifyingGlass from '~icons/heroicons/magnifying-glass-16-solid';
	import IconUser from '~icons/heroicons/user';
	import IconSquaresSolid from '~icons/heroicons/squares-2x2-16-solid';
	import { Separator } from '$lib/components/ui/separator';

	type Props = {
		navItems: Entity[];
		user: { name: string; avatar: string };
	};

	const { navItems, user }: Props = $props();

	let open = $state(false);
	let searchResults: Model[] = $state([]);
	let isMac: boolean | undefined = $state(undefined);

	const api = new Api();

	const debouncedSearch = debounce(async () => {
		if (input.length > 0) {
			const response = await api.search(input);
			searchResults = response.items;
		} else {
			searchResults = [];
		}
	}, 300);

	onDestroy(() => {
		debouncedSearch.cancel();
	});

	let input = $state('');

	function isActiveRoute(searchString: string): boolean {
		return $page.url.pathname.startsWith(searchString);
	}

	function handleKeydown(event: KeyboardEvent) {
		if ((event.metaKey || event.ctrlKey) && event.key === 'k') {
			event.preventDefault();
			open = true;
		}
	}

	onMount(() => {
		isMac = isMacOS();
		document.addEventListener('keydown', handleKeydown);
		return () => {
			document.removeEventListener('keydown', handleKeydown);
		};
	});

	function handleSelect(path: string) {
		goto(path);
		open = false;
	}
</script>

<nav class="w-60 p-3 flex flex-col">
	<div class="ml-2 mt-1 mb-10 flex items-center justify-between">
		<Button variant="link" href="/" class="p-0 h-6 w-6">
			<img src="/logo.png" alt="Zipline Logo" />
		</Button>
	</div>
	<Button
		variant="outline"
		size="sm"
		class="mb-6 w-full flex justify-start pl-2"
		onclick={() => (open = true)}
		icon="leading"
	>
		<IconMagnifyingGlass class="text-foreground" />
		<span class="text-muted-foreground">Search</span>
		{#if isMac !== undefined}
			<span class="ml-auto text-xs text-muted-foreground">
				{#if isMac}
					<Badge variant="key-bg">⌘</Badge>
					<Badge variant="key-bg">K</Badge>
				{:else}
					<Badge variant="key-bg">Ctrl</Badge>
					<Badge variant="key-bg">K</Badge>
				{/if}
			</span>
		{/if}
	</Button>
	<Button variant="ghost" class="text-regular-medium" size="nav" href="/" icon="leading">
		<IconSquaresSolid class="text-muted-icon-neutral" />
		<span class="text-muted-foreground">Home</span>
	</Button>
	<Separator class="my-6" />
	<span class="mb-[10px] px-2 text-xs-medium text-muted-foreground">Datasets</span>
	<ul class="space-y-1 flex-grow">
		{#each navItems as item}
			<li>
				<Button
					variant={isActiveRoute(item.path) ? 'default' : 'ghost'}
					size="nav"
					href={item.path}
					icon="leading"
				>
					<item.icon
						class={isActiveRoute(item.path) ? 'text-muted-icon-primary' : 'text-muted-icon-neutral'}
					/>
					{item.label}
				</Button>
			</li>
		{/each}
	</ul>
	<Separator class="my-6" />
	<span class="mb-[10px] px-2 text-xs-medium text-muted-foreground">Resources</span>
	<Button
		variant="ghost"
		class="w-full text-regular-medium my-1"
		size="nav"
		href="https://docs.chronon.ai"
		target="_blank"
		rel="noopener noreferrer"
		icon="leading"
	>
		<IconDocumentText class="text-muted-icon-neutral" />
		<span class="text-muted-foreground">Chronon docs</span>
	</Button>
	<Button
		variant="ghost"
		class="w-full text-regular-medium my-1"
		size="nav"
		href="mailto:hello@zipline.ai"
		icon="leading"
	>
		<IconDocumentText class="text-muted-icon-neutral" />
		<span class="text-muted-foreground">Support</span>
	</Button>
	<Separator class="mt-6 mb-4" />
	<div class="flex items-center">
		<DropdownMenu>
			<DropdownMenuTrigger asChild let:builder>
				<Button variant="ghost" class="flex items-center cursor-pointer" builders={[builder]}>
					<Avatar class="h-4 w-4">
						<AvatarImage src={user.avatar} alt={user.name} />
						<AvatarFallback>
							<IconUser />
						</AvatarFallback>
					</Avatar>
					<span class="ml-3">{user.name}</span>
					<IconChevronDown class="ml-3" />
				</Button>
			</DropdownMenuTrigger>
			<DropdownMenuContent>
				<DropdownMenuItem onclick={() => alert('Settings clicked')}>Settings</DropdownMenuItem>
				<DropdownMenuItem onclick={() => alert('Sign out clicked')}>Sign out</DropdownMenuItem>
			</DropdownMenuContent>
		</DropdownMenu>
	</div>
</nav>

<CommandDialog bind:open>
	<CommandInput
		placeholder="Type a command or search..."
		oninput={debouncedSearch}
		bind:value={input}
	/>
	<CommandList>
		<CommandEmpty>No results found</CommandEmpty>
		{#if searchResults.length === 0}
			{#if input === ''}
				<CommandGroup heading="Quick actions">
					<CommandItem disabled>
						<IconExclamationTriangle />
						Show only models with alerts</CommandItem
					>
					<CommandItem disabled>
						<IconAdjustmentsHorizontal />
						Filter by...</CommandItem
					>
					<CommandItem disabled>
						<IconArrowsUpDown />
						Sort by...</CommandItem
					>
				</CommandGroup>
				<CommandGroup heading="Learn">
					<CommandItem onSelect={() => window.open('https://docs.chronon.ai', '_blank')}>
						<IconDocumentText />
						Chronon docs</CommandItem
					>
				</CommandGroup>
			{/if}
		{:else}
			<CommandGroup heading={`Search for "${input}"`}>
				{#each searchResults as entity (entity.name)}
					<!-- todo: enable this once we have data for all joins -->
					<CommandItem
						disabled={entity.name !== 'risk.user_transactions.txn_join'}
						onSelect={() =>
							handleSelect(`${getEntity('joins').path}/${encodeURIComponent(entity.name)}`)}
					>
						{@const IconJoins = getEntity('joins').icon}
						<IconJoins />
					</CommandItem>
				{/each}
			</CommandGroup>
		{/if}
	</CommandList>
</CommandDialog>
