<script lang="ts">
	import '../app.css';
	import { type Snippet } from 'svelte';
	import { Avatar, AvatarFallback, AvatarImage } from '$lib/components/ui/avatar';
	import { Button } from '$lib/components/ui/button';
	import { Person } from 'svelte-radix';
	import { navigating } from '$app/stores';
	import { expoOut } from 'svelte/easing';
	import { slide } from 'svelte/transition';

	let { children }: { children: Snippet } = $props();

	// TODO: Replace with actual user data
	const user = {
		name: 'Demo User',
		avatar: '/path/to/avatar.jpg'
	};

	// TODO: Replace with actual navigation items
	const navItems = [
		{ label: 'Models', href: '/models' },
		{ label: 'GroupBys', href: '/groupbys' },
		{ label: 'Joins', href: '/joins' }
	];
</script>

<div class="flex h-screen flex-col">
	{#if $navigating}
		<div
			class="fixed w-full top-0 right-0 left-0 h-[1px] z-50 bg-primary"
			in:slide={{ duration: 2000, axis: 'x', easing: expoOut }}
		></div>
	{/if}
	<!-- Top bar -->
	<header class="flex items-center justify-between border-b px-4 py-2 bg-muted/40">
		<div class="flex items-center">
			<Button variant="ghost" href="/" class="p-0">
				<img src="/logo.webp" alt="Zipline Logo" class="h-8 w-auto" />
			</Button>
		</div>
		<div class="flex items-center">
			<span class="mr-2">{user.name}</span>
			<Avatar>
				<AvatarImage src={user.avatar} alt={user.name}></AvatarImage>
				<AvatarFallback>
					<Person></Person>
				</AvatarFallback>
			</Avatar>
		</div>
	</header>

	<div class="flex flex-1 overflow-hidden">
		<!-- Left navigation -->
		<nav class="w-48 p-4 bg-muted/40">
			<ul class="space-y-2">
				{#each navItems as item}
					<li>
						<Button variant="ghost" class="w-full justify-start" href={item.href}>
							{item.label}
						</Button>
					</li>
				{/each}
			</ul>
		</nav>

		<!-- Main content -->
		<main class="flex-1 overflow-auto p-4" data-testid="app-main">
			{@render children()}
		</main>
	</div>
</div>
