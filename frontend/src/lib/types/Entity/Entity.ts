import type { ValueOf } from '@layerstack/utils';
import {
	type IJoinArgs,
	type IGroupByArgs,
	type IModelArgs,
	type IStagingQueryArgs,
	ConfType,
	LogicalType,
	type ISourceArgs,
	type IJoinSourceArgs,
	type ITabularDataArgs
} from '$lib/types/codegen';

import IconCube from '~icons/heroicons/cube-16-solid';
import IconSquare3Stack3d from '~icons/heroicons/square-3-stack-3d-16-solid';
import IconCubeTransparent from '~icons/heroicons/cube-transparent-16-solid';
import IconRectangleStack from '~icons/heroicons/rectangle-stack-16-solid';
import IconTableCells from '~icons/heroicons/table-cells-16-solid';
import IconSignal from '~icons/heroicons/signal-16-solid';

export const EntityType = {
	// Conf
	MODEL: 'MODEL',
	JOIN: 'JOIN',
	GROUP_BY: 'GROUP_BY',
	STAGING_QUERY: 'STAGING_QUERY',

	// Tablular Data
	ENTITY_SOURCE: 'ENTITY_SOURCE',
	EVENT_SOURCE: 'EVENT_SOURCE',
	JOIN_SOURCE: 'JOIN_SOURCE'
} as const;

export const entityConfig = {
	[EntityType.MODEL]: {
		label: 'Models',
		confType: ConfType.MODEL,
		logicalType: LogicalType.MODEL,
		path: '/models',
		icon: IconCube,
		color: '200 80% 50%'
	},
	[EntityType.JOIN]: {
		label: 'Joins',
		confType: ConfType.JOIN,
		logicalType: LogicalType.JOIN,
		path: '/joins',
		icon: IconSquare3Stack3d,
		color: '100 80% 50%'
	},
	[EntityType.GROUP_BY]: {
		label: 'GroupBys',
		confType: ConfType.GROUP_BY,
		logicalType: LogicalType.GROUP_BY,
		path: '/groupbys',
		icon: IconRectangleStack,
		color: '50 80% 50%'
	},
	[EntityType.STAGING_QUERY]: {
		label: 'Staging Queries',
		confType: ConfType.STAGING_QUERY,
		logicalType: LogicalType.STAGING_QUERY,
		path: '/stagingqueries',
		icon: IconCubeTransparent,
		color: '150 80% 50%'
	},
	[EntityType.ENTITY_SOURCE]: {
		label: 'Entity Sources',
		confType: null,
		logicalType: LogicalType.TABULAR_DATA,
		path: null,
		icon: IconTableCells,
		color: '220 80% 50%'
	},
	[EntityType.EVENT_SOURCE]: {
		label: 'Event Sources',
		confType: null,
		logicalType: LogicalType.TABULAR_DATA,
		path: null,
		icon: IconSignal,
		color: '220 80% 50%'
	},
	[EntityType.JOIN_SOURCE]: {
		label: 'Join Sources',
		confType: null,
		logicalType: LogicalType.TABULAR_DATA,
		path: null,
		icon: IconSquare3Stack3d,
		color: '100 80% 50%'
	}
} as const;

export type EntityConfig = ValueOf<typeof entityConfig>;

export type EntityData = NonNullable<
	IModelArgs &
		IJoinArgs &
		IGroupByArgs &
		IStagingQueryArgs &
		ITabularDataArgs & // TODO: Remove this
		IJoinSourceArgs & // TODO: Remove this
		ISourceArgs['entities'] &
		ISourceArgs['events'] &
		ISourceArgs['joinSource']
>;

export function getEntityType(entity: EntityData) {
	if ('joinParts' in entity) {
		return EntityType.JOIN;
	} else if ('sources' in entity) {
		return EntityType.GROUP_BY;
	} else if ('query' in entity && typeof entity.query === 'string') {
		return EntityType.STAGING_QUERY;
	} else if ('modelType' in entity) {
		return EntityType.MODEL;
	} else if ('snapshotTable' in entity) {
		return EntityType.ENTITY_SOURCE;
	} else if ('table' in entity) {
		return EntityType.EVENT_SOURCE;
	} else if ('join' in entity) {
		return EntityType.JOIN_SOURCE;
	} else {
		console.error('Unknown entity type', entity);
		throw new Error('Unknown entity type');
	}
}

export function getEntityConfig(entity: EntityData) {
	const entityType = getEntityType(entity);
	return entityConfig[entityType];
}
