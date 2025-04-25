<script lang="ts">
	import {
		Button,
		Dialog,
		Kbd,
		Menu,
		MenuItem,
		NavItem,
		SelectField,
		Toggle,
		type MenuOption
	} from 'svelte-ux';
	import { flatRollup } from 'd3';
	import { autoFocus } from '@layerstack/svelte-actions';
	import { cls } from '@layerstack/tailwind';

	import { page } from '$app/state';
	import { Api } from '$lib/api/api';
	import { goto } from '$app/navigation';
	import { isMacOS } from '$lib/util/browser';
	import { entityConfig, getEntityConfig } from '$lib/types/Entity';
	import Separator from '$lib/components/Separator.svelte';
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
</script>

<nav class="w-60 p-3 grid grid-rows-[auto_1fr_auto]">
	<div>
		<div class="ml-2 mt-1 mb-10 flex items-center justify-between">
			<a href="/">
				<Logo class="h-6 w-6" />
			</a>
		</div>

		<Button
			size="sm"
			class="mb-[22px] pl-2 border border-neutral-400"
			fullWidth
			onclick={() => (open = true)}
		>
			<IconMagnifyingGlass class="text-foreground" />
			<span class="text-surface-content/50">Search</span>
			<span class="ml-auto">
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
		<NavItem path="/" currentUrl={page.url}>
			<IconSquaresSolid />
			Home
		</NavItem>

		<Separator class="my-4" />

		<div class="mb-2 px-2 text-xs font-medium text-surface-content/40">Datasets</div>

		{#each navItems.filter((item) => item.path != null) as item}
			<NavItem path={item.path} currentUrl={page.url}>
				<item.icon />
				{item.label}
			</NavItem>
		{/each}
	</div>

	<div>
		<Separator class="mb-6" />

		<div class="mb-2 px-2 text-xs font-medium text-surface-content/40">Resources</div>

		<NavItem path="https://docs.chronon.ai" currentUrl={page.url} target="_blank">
			<IconDocumentText />
			<span class="text-surface-content/60">Chronon docs</span>
		</NavItem>

		<NavItem
			path="https://join.slack.com/t/chrononworkspace/shared_invite/zt-1no5t3lxd-mFUo644T6tPJOeTeZRTggw"
			currentUrl={page.url}
			target="_blank"
		>
			<IconQuestionMarkCircle16Solid />
			<span class="text-surface-content/60">Support</span>
		</NavItem>

		<Button href="mailto:hello@zipline.ai" class="border border-neutral-400 px-3 ml-2 mt-5">
			<IconChatBubbleOvalLeft16Solid class="text-surface-content/40" />
			<span class="text-surface-content/50">Got feedback?</span>
		</Button>

		<Separator class="mt-[19px] mb-3" />

		<Toggle let:on={open} let:toggle let:toggleOff>
			<Button on:click={toggle}>
				<IconUser class="text-surface-content/50" />
				<span class="ml-3 text-sm text-surface-content/70">{user.name}</span>
				<IconChevronDown
					class={cls('ml-3 text-surface-content/50 transition-transform', open && '-rotate-180')}
				/>

				<Menu {open} on:close={toggleOff} matchWidth class="p-1">
					<MenuItem on:click={() => alert('Settings clicked')}>Settings</MenuItem>
					<MenuItem on:click={() => alert('Sign out clicked')}>Sign out</MenuItem>
				</Menu>
			</Button>
		</Toggle>
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
