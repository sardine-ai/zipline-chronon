import {
	LogicalType,
	type IGroupByArgs,
	type IJoinArgs,
	type IJoinSourceArgs,
	type ILogicalNodeArgs,
	type IModelArgs,
	type INodeInfoArgs,
	type INodeKeyArgs,
	type ISourceArgs,
	type IStagingQueryArgs,
	type ITabularDataArgs
} from '$lib/types/codegen';
import { EntityTypes, getEntity } from '$lib/types/Entity/Entity';

import IconTableCells from '~icons/heroicons/table-cells-16-solid';
import IconSignal from '~icons/heroicons/signal-16-solid';

/** All unioned field properties combined into a single type */
export type CombinedLogicalNode = Required<{
	[K in keyof ILogicalNodeArgs]: NonNullable<ILogicalNodeArgs[K]>;
}>[keyof ILogicalNodeArgs];

export type NodeConfiguration = NonNullable<
	IStagingQueryArgs &
		IJoinArgs &
		IGroupByArgs &
		IModelArgs &
		ITabularDataArgs &
		IJoinSourceArgs &
		ISourceArgs['entities'] &
		ISourceArgs['events'] &
		ISourceArgs['joinSource']
>;

export const logicalNodeConfig = {
	[LogicalType.GROUP_BY]: {
		label: 'GroupBys',
		icon: getEntity(EntityTypes.GROUPBYS).icon,
		color: '50 80% 50%',
		url: '/groupbys'
	},
	[LogicalType.JOIN]: {
		label: 'Joins',
		icon: getEntity(EntityTypes.JOINS).icon,
		color: '100 80% 50%',
		url: '/joins'
	},
	[LogicalType.STAGING_QUERY]: {
		label: 'Staging Queries',
		icon: getEntity(EntityTypes.STAGINGQUERIES).icon,
		color: '150 80% 50%',
		url: '/stagingqueries'
	},
	[LogicalType.MODEL]: {
		label: 'Models',
		icon: getEntity(EntityTypes.MODELS).icon,
		color: '200 80% 50%',
		url: '/models'
	},
	[LogicalType.TABULAR_DATA]: {
		label: 'Tabular Data',
		icon: IconTableCells,
		color: '220 80% 50%',
		url: null // Not available
	}
};

/** Determine node type from it's properties */
export function getLogicalNodeType(node: CombinedLogicalNode) {
	if ('joinParts' in node) {
		return LogicalType.JOIN;
	} else if ('sources' in node) {
		return LogicalType.GROUP_BY;
	} else if ('query' in node && typeof node.query === 'string') {
		return LogicalType.STAGING_QUERY;
	} else if ('modelType' in node) {
		return LogicalType.MODEL;
	} else {
		return LogicalType.TABULAR_DATA;
	}
}

/**
 * Returns the logical source type of a node (`source.entities` or `source.events`) if node is a source, else `null`
 */
export function getLogicalNodeSourceType(node: CombinedLogicalNode) {
	return 'snapshotTable' in node ? 'entity' : 'table' in node ? 'event' : null;
}

export function getLogicalNodeConfig(node: { key: INodeKeyArgs; value: INodeInfoArgs }) {
	const nodeType = getLogicalNodeType(node.value.conf as CombinedLogicalNode); // TODO: Is not `ILogicalNode` with nested `join` | `groupBy` | etc properties
	if (nodeType === LogicalType.TABULAR_DATA) {
		return {
			...logicalNodeConfig[LogicalType.TABULAR_DATA],
			icon:
				getLogicalNodeSourceType(node.value.conf as CombinedLogicalNode) === 'entity'
					? IconTableCells
					: IconSignal
		};
	} else {
		return logicalNodeConfig[node.key.logicalType!];
	}
}

/**
 * Returns true if the node is a streaming node
 */
export function isStreaming(node: CombinedLogicalNode): boolean {
	if ('topic' in node || 'mutationTopic' in node) {
		return true;
	} else if ('table' in node || 'snapshotTable' in node) {
		// Source without topic
		return false;
	} else if ('left' in node && node.left) {
		// Join
		return isSourceStreaming(node.left);
	} else if ('sources' in node) {
		// Check if any upstream sources are streaming
		return node.sources?.some((source) => isSourceStreaming(source)) ?? false;
	} else if ('join' in node) {
		// Check if any upstream joins are streaming
		return isStreaming(node.join!);
	} else if ('joinParts' in node) {
		// Check if any upstream joinParts are streaming
		return node.joinParts?.some((joinPart) => isStreaming(joinPart.groupBy!)) ?? true;
	} else {
		return false;
	}
}

function isSourceStreaming(source: ISourceArgs) {
	if ('entities' in source && source.entities) {
		return isStreaming(source.entities);
	} else if ('events' in source && source.events) {
		return isStreaming(source.events);
	} else if ('joinSource' in source && source.joinSource) {
		return isStreaming(source.joinSource.join!);
	}
	return false;
}
