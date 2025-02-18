import { LogicalType, type ILogicalNode, type INodeInfo, type INodeKey } from './codegen';
import { EntityTypes, getEntity } from './Entity/Entity';

import IconTableCells from '~icons/heroicons/table-cells-16-solid';
import IconSignal from '~icons/heroicons/signal-16-solid';

/** All unioned field properties combined into a single type */
export type CombinedLogicalNode = Required<{
	[K in keyof ILogicalNode]: NonNullable<ILogicalNode[K]>;
}>[keyof ILogicalNode];

export const logicalNodeConfig = {
	[LogicalType.GROUP_BY]: {
		label: 'GroupBys',
		icon: getEntity(EntityTypes.GROUPBYS).icon,
		color: '50 80% 50%'
	},
	[LogicalType.JOIN]: {
		label: 'Joins',
		icon: getEntity(EntityTypes.JOINS).icon,
		color: '100 80% 50%'
	},
	[LogicalType.STAGING_QUERY]: {
		label: 'Staging Queries',
		icon: getEntity(EntityTypes.STAGINGQUERIES).icon,
		color: '150 80% 50%'
	},
	[LogicalType.MODEL]: {
		label: 'Models',
		icon: getEntity(EntityTypes.MODELS).icon,
		color: '200 80% 50%'
	},
	[LogicalType.TABULAR_DATA]: {
		label: 'Tabular Data',
		icon: IconTableCells,
		color: '220 80% 50%'
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

export function getLogicalNodeConfig(node: { key: INodeKey; value: INodeInfo }) {
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
	} else if ('sources' in node) {
		// Check if any upstream sources are streaming
		return (
			node.sources?.some((source) => {
				if ('entities' in source && source.entities) {
					return isStreaming(source.entities);
				} else if ('events' in source && source.events) {
					return isStreaming(source.events);
				} else if ('joinSource' in source && source.joinSource) {
					return isStreaming(source.joinSource as CombinedLogicalNode);
				}
			}) ?? false
		);
	} else if ('join' in node) {
		// Check if any upstream joins are streaming
		// @ts-expect-error - TODO: fix types
		return node.join.joinParts?.some((joinPart) => isStreaming(joinPart.groupBy)) ?? true;
	} else if ('joinParts' in node) {
		// Check if any upstream joinParts are streaming
		// @ts-expect-error - TODO: fix types
		return node.joinParts?.some((joinPart) => isStreaming(joinPart.groupBy)) ?? true;
	} else {
		return false;
	}
}
