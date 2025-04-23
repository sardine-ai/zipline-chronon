<script lang="ts">
	import { Dialog, Kbd, Menu, MenuItem, SelectField, Toggle, type MenuOption } from 'svelte-ux';
	import { flatRollup } from 'd3';
	import { autoFocus } from '@layerstack/svelte-actions';
	import { cls } from '@layerstack/tailwind';

	import { Button } from '$lib/components/ui/button';
	import { page } from '$app/state';
	import { Api } from '$lib/api/api';
	import { goto } from '$app/navigation';
	import { isMacOS } from '$lib/util/browser';
	import { entityConfig, getEntityConfig } from '$lib/types/Entity';
	import { Separator } from '$lib/components/ui/separator';
	import Logo from '$lib/components/Logo.svelte';

	import IconChevronDown from '~icons/heroicons/chevron-down';
	import IconDocumentText from '~icons/heroicons/document-text-16-solid';
	import IconMagnifyingGlass from '~icons/heroicons/magnifying-glass-16-solid';
	import IconUser from '~icons/heroicons/user';
	import IconSquaresSolid from '~icons/heroicons/squares-2x2-16-solid';
	import IconChatBubbleOvalLeft16Solid from '~icons/heroicons/chat-bubble-oval-left-16-solid';
	import IconQuestionMarkCircle16Solid from '~icons/heroicons/question-mark-circle-16-solid';

	type Props = {
		user: { name: string; avatar: string };
	};

	const { user }: Props = $props();

	let open = $state(false);

	const api = new Api();

	const navItems = Object.values(entityConfig).filter((c) => c.confType != null);

	const search = async (searchText: string) => {
		if (searchText.length > 0) {
			const entities = await api.search(searchText).then((res) => {
				// Combine all entities into single result set
				return [
					...(res.models ?? []),
					...(res.joins ?? []),
					...(res.groupBys ?? []),
					...(res.stagingQueries ?? [])
				];
			});

			const _options = entities.map((entity) => {
				const config = getEntityConfig(entity);
				return {
					label: entity.metaData?.name ?? 'Unknown',
					value: `${config.path}/${encodeURIComponent(entity.metaData?.name || '')}`,
					icon: config.icon,
					group: 'Results',
					config: config
				};
			});

			// TODO: Remove dedupe after data is improved
			const options = flatRollup(
				_options,
				(values) => values[0],
				(d) => d.label
			).map((d) => d[1]);

			return options;
		} else {
			return [];
		}
	};

	function isActiveRoute(searchString: string): boolean {
		return page.url.pathname.startsWith(searchString);
	}
</script>

<nav class="w-60 p-3 grid grid-rows-[auto_1fr_auto]">
	<div>
		<div class="ml-2 mt-1 mb-10 flex items-center justify-between">
			<Button variant="link" href="/" class="p-0 h-6 w-6">
				<Logo class="text-foreground" />
			</Button>
		</div>

		<Button
			variant="outline"
			size="sm"
			class="mb-[22px] w-full flex justify-start pl-2"
			onclick={() => (open = true)}
			icon="leading"
		>
			<IconMagnifyingGlass class="text-foreground" />
			<span class="text-muted-foreground">Search</span>
			<span class="ml-auto text-xs text-muted-foreground">
				<Kbd
					command={isMacOS()}
					control={!isMacOS()}
					class="size-5 p-0 items-center justify-center"
				/>
				<Kbd class="size-5 p-0 items-center justify-center font-semibold">K</Kbd>
			</span>
		</Button>
	</div>

	<div>
		<Button variant="ghost" class="text-sm" size="nav" href="/" icon="leading">
			<IconSquaresSolid class="text-muted-icon-neutral h-4 w-4" />
			<span>Home</span>
		</Button>

		<Separator class="my-4" />

		<span class="mb-[10px] px-2 text-xs font-medium text-muted-icon-neutral">Datasets</span>

		<ul>
			{#each navItems.filter((item) => item.path != null) as item}
				<li>
					<Button
						variant={isActiveRoute(item.path) ? 'default' : 'ghost'}
						size="nav"
						href={item.path}
						icon="leading"
						class="text-sm"
					>
						<item.icon
							class={`h-4 w-4 ${isActiveRoute(item.path) ? 'text-muted-icon-primary' : 'text-muted-icon-neutral'}`}
						/>
						{item.label}
					</Button>
				</li>
			{/each}
		</ul>
	</div>

	<div>
		<Separator class="mb-6" />

		<span class="mb-[7px] px-2 text-xs font-medium text-muted-icon-neutral">Resources</span>

		<Button
			variant="ghost"
			class="w-full text-sm my-[2px]"
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
			class="w-full text-sm my-[2px]"
			size="nav"
			href="https://join.slack.com/t/chrononworkspace/shared_invite/zt-1no5t3lxd-mFUo644T6tPJOeTeZRTggw"
			target="_blank"
			rel="noopener noreferrer"
			icon="leading"
		>
			<IconQuestionMarkCircle16Solid class="text-muted-icon-neutral" />
			<span class="text-muted-foreground">Support</span>
		</Button>

		<div class="flex justify-start">
			<Button
				variant="outline"
				size="sm"
				class="text-[13px] mb-1 mt-5 ml-2"
				href="mailto:hello@zipline.ai"
				icon="leading"
			>
				<IconChatBubbleOvalLeft16Solid class="text-muted-icon-neutral" />
				<span class="text-muted-foreground">Got feedback?</span>
			</Button>
		</div>

		<Separator class="mt-[19px] mb-3" />

		<div class="flex items-center">
			<Toggle let:on={open} let:toggle let:toggleOff>
				<Button variant="ghost" on:click={toggle}>
					<IconUser class="text-muted-foreground" />
					<span class="ml-3 text-muted-foreground text-sm">{user.name}</span>
					<IconChevronDown
						class={cls('ml-3 text-muted-foreground transition-transform', open && '-rotate-180')}
					/>

					<Menu {open} on:close={toggleOff} matchWidth class="p-1">
						<MenuItem on:click={() => alert('Settings clicked')}>Settings</MenuItem>
						<MenuItem on:click={() => alert('Sign out clicked')}>Sign out</MenuItem>
					</Menu>
				</Button>
			</Toggle>
		</div>
	</div>
</nav>

<Dialog bind:open classes={{ backdrop: 'backdrop-blur-xs' }}>
	<SelectField
		placeholder="Search..."
		options={[
			{ label: 'Home', value: '/', group: 'Quick actions', icon: IconSquaresSolid },
			{
				label: 'Chronon docs',
				value: 'https://docs.chronon.ai',
				group: 'Quick actions',
				icon: IconDocumentText
			}
		] as unknown as MenuOption<string>[]}
		inlineOptions
		search={async (text) => {
			const options = await search(text);
			return options as unknown as MenuOption<string>[]; // TODO: Workaround until `icon` accepts component
		}}
		on:change={(e) => {
			open = false;
			if (e.detail.value?.startsWith('http')) {
				window.open(e.detail.value, '_blank');
			} else {
				goto(e.detail.value ?? '/');
			}
		}}
		classes={{
			root: 'w-[500px] max-w-[95vw] py-1 border rounded-md',
			field: {
				container: 'border-none hover:shadow-none group-focus-within:shadow-none'
			},
			options: 'overflow-auto max-h-[min(90dvh,380px)]',
			group: 'capitalize'
		}}
		fieldActions={(node) => [autoFocus(node)]}
	>
		<svelte:fragment slot="prepend">
			<IconMagnifyingGlass class="mr-2" />
		</svelte:fragment>

		<svelte:fragment slot="option" let:option let:highlightIndex let:index>
			<MenuItem
				class={cls(index === highlightIndex && '[:not(.group:hover)>&]:bg-surface-content/5')}
				scrollIntoView={{
					condition: index === highlightIndex,
					onlyIfNeeded: true
				}}
			>
				{@const config = option.config}
				{@const Icon = option.icon}

				<div class="h-full px-2 grid grid-cols-[auto_1fr] gap-3 items-center">
					{#if config}
						{@const [namespace, ...nameParts] = option.label?.split('.') ?? []}
						<div
							style:--color={config.color}
							class="bg-[hsl(var(--color)/5%)] border border-[hsl(var(--color))] text-[hsl(var(--color))] w-8 h-8 rounded flex items-center justify-center"
						>
							<Icon />
						</div>
						<div class="align-middle truncate">
							<div class="text-xs text-surface-content/50">
								{namespace}
							</div>
							<div class="text-sm text-surface-content truncate">
								{nameParts.join('.')}
							</div>
						</div>
					{:else}
						<Icon class="text-surface-content/50" />
						<div class="text-sm text-surface-content truncate">
							{option.label}
						</div>
					{/if}
				</div>
			</MenuItem>
		</svelte:fragment>
	</SelectField>
</Dialog>

<svelte:window
	onkeydown={(e) => {
		if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
			e.preventDefault();
			open = true;
		}
	}}
/>
