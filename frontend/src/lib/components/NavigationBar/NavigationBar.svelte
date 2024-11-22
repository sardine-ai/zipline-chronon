<script lang="ts">
	import { Avatar, AvatarFallback, AvatarImage } from '$lib/components/ui/avatar';
	import { Button } from '$lib/components/ui/button';
	import { page } from '$app/stores';
	import {
		CommandDialog,
		CommandInput,
		CommandList,
		CommandGroup,
		CommandItem
	} from '$lib/components/ui/command/';
	import { search } from '$lib/api/api';
	import type { Model } from '$lib/types/Model/Model';
	import debounce from 'lodash/debounce';
	import { onDestroy, onMount } from 'svelte';
	import {
		DropdownMenu,
		DropdownMenuTrigger,
		DropdownMenuContent,
		DropdownMenuItem
	} from '$lib/components/ui/dropdown-menu/';
	import {
		Icon,
		ChevronDown,
		MagnifyingGlass,
		User,
		DocumentText,
		ExclamationTriangle,
		AdjustmentsHorizontal,
		ArrowsUpDown
	} from 'svelte-hero-icons';
	import type { IconSource } from 'svelte-hero-icons';
	import { goto } from '$app/navigation';
	import { isMacOS } from '$lib/util/browser';
	import { Badge } from '$lib/components/ui/badge';

	type Props = {
		navItems: { label: string; href: string; icon: IconSource }[];
		user: { name: string; avatar: string };
	};

	const { navItems, user }: Props = $props();

	let open = $state(false);
	let searchResults: Model[] = $state([]);
	let isMac: boolean | undefined = $state(undefined);

	const debouncedSearch = debounce(async () => {
		if (input.length > 0) {
			const response = await search(input);
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
		<Icon src={MagnifyingGlass} micro size="16" class="text-foreground" />
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
	<Button
		variant="ghost"
		class="mb-9 text-regular-medium"
		size="nav"
		href="https://docs.chronon.ai"
		target="_blank"
		rel="noopener noreferrer"
		icon="leading"
	>
		<Icon src={DocumentText} micro size="16" class="text-muted-icon-neutral" />
		<span class="text-muted-foreground">Chronon docs</span>
	</Button>
	<span class="mb-[10px] px-2 text-xs-medium text-muted-foreground">Observability</span>
	<ul class="space-y-1 flex-grow">
		{#each navItems as item}
			<li>
				<Button
					variant={isActiveRoute(item.href) ? 'default' : 'ghost'}
					size="nav"
					href={item.href}
					icon="leading"
				>
					<Icon
						src={item.icon}
						micro
						size="16"
						class={isActiveRoute(item.href) ? 'text-muted-icon-primary' : 'text-muted-icon-neutral'}
					/>
					{item.label}
				</Button>
			</li>
		{/each}
	</ul>
	<div class="flex items-center mt-auto">
		<DropdownMenu>
			<DropdownMenuTrigger>
				<Button variant="ghost" class="flex items-center cursor-pointer">
					<Avatar class="h-4 w-4">
						<AvatarImage src={user.avatar} alt={user.name} />
						<AvatarFallback>
							<Icon src={User} />
						</AvatarFallback>
					</Avatar>
					<span class="ml-3">{user.name}</span>
					<Icon src={ChevronDown} size="16" class="ml-3" />
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
		{#if searchResults.length === 0}
			{#if input === ''}
				<CommandGroup heading="Quick actions">
					<CommandItem disabled>
						<Icon src={ExclamationTriangle} micro size="16" />
						Show only models with alerts</CommandItem
					>
					<CommandItem disabled>
						<Icon src={AdjustmentsHorizontal} micro size="16" />
						Filter by...</CommandItem
					>
					<CommandItem disabled>
						<Icon src={ArrowsUpDown} micro size="16" />
						Sort by...</CommandItem
					>
				</CommandGroup>
				<CommandGroup heading="Learn">
					<CommandItem onSelect={() => window.open('https://docs.chronon.ai', '_blank')}>
						<Icon src={DocumentText} micro size="16" />
						Chronon docs</CommandItem
					>
				</CommandGroup>
			{/if}
		{:else}
			<CommandGroup heading={`Search for "${input}"`}>
				{#each searchResults as model}
					<CommandItem onSelect={() => handleSelect(`/models/${encodeURIComponent(model.name)}`)}>
						{model.name}
					</CommandItem>
				{/each}
			</CommandGroup>
		{/if}
	</CommandList>
</CommandDialog>
