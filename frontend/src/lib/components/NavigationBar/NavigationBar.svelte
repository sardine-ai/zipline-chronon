<script lang="ts">
	import { Avatar, AvatarFallback, AvatarImage } from '$lib/components/ui/avatar';
	import { Button } from '$lib/components/ui/button';
	import { Person, MagnifyingGlass } from 'svelte-radix';
	import { page } from '$app/stores';
	import {
		CommandDialog,
		CommandInput,
		CommandList,
		CommandEmpty,
		CommandGroup,
		CommandItem
	} from '$lib/components/ui/command/';
	import { search } from '$lib/api/api';
	import type { Model } from '$lib/types/Model/Model';
	import debounce from 'lodash/debounce';
	import { onDestroy } from 'svelte';

	type Props = {
		navItems: { label: string; href: string }[];
		user: { name: string; avatar: string };
	};

	const { navItems, user }: Props = $props();

	let open = $state(false);
	let searchResults: Model[] = $state([]);

	const debouncedSearch = debounce(async () => {
		console.log('searching', input);
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
</script>

<nav class="w-64 p-4 flex flex-col">
	<div class="mb-4 flex items-center justify-between">
		<Button variant="ghost" href="/" class="p-0">
			<img src="/logo.webp" alt="Zipline Logo" class="h-8 w-auto" />
		</Button>
	</div>
	<Button
		variant="outline"
		class="mb-4 w-full flex justify-start pl-2"
		onclick={() => (open = true)}
	>
		<MagnifyingGlass class="mr-2" />
		<span>Search</span>
	</Button>
	<ul class="space-y-1 flex-grow">
		{#each navItems as item}
			<li>
				<Button
					variant="ghost"
					class="w-full justify-start h-7 px-2 {$page.url.pathname === item.href
						? 'bg-primary/10 text-primary'
						: 'hover:bg-primary/5'}"
					href={item.href}
				>
					{item.label}
				</Button>
			</li>
		{/each}
	</ul>
	<div class="flex items-center mt-auto">
		<span class="mr-2">{user.name}</span>
		<Avatar>
			<AvatarImage src={user.avatar} alt={user.name} />
			<AvatarFallback>
				<Person />
			</AvatarFallback>
		</Avatar>
	</div>
</nav>

<CommandDialog bind:open>
	<CommandInput placeholder="Type to search..." oninput={debouncedSearch} bind:value={input} />
	<CommandList>
		{#if searchResults.length === 0}
			{#if input === ''}
				<CommandEmpty>Type to search by model name.</CommandEmpty>
			{:else}
				<CommandEmpty>No results found.</CommandEmpty>
			{/if}
		{:else}
			<CommandGroup heading="Search Results">
				{#each searchResults as model}
					<CommandItem>
						<Button
							variant="ghost"
							href="/models/{model.id}/observability"
							onclick={() => (open = false)}
							class="w-full justify-start"
						>
							{model.name}
						</Button>
					</CommandItem>
				{/each}
			</CommandGroup>
		{/if}
	</CommandList>
</CommandDialog>
