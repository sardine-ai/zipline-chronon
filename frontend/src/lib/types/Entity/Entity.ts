import IconCube from '~icons/heroicons/cube-16-solid';
import IconSquare3Stack3d from '~icons/heroicons/square-3-stack-3d-16-solid';
import IconCubeTransparent from '~icons/heroicons/cube-transparent-16-solid';
import IconRectangleStack from '~icons/heroicons/rectangle-stack-16-solid';
import {
	type IJoinArgs,
	type IGroupByArgs,
	type IModelArgs,
	type IStagingQueryArgs,
	ConfType
} from '$lib/types/codegen';

export const EntityTypes = {
	MODELS: 'models',
	JOINS: 'joins',
	GROUPBYS: 'groupbys',
	STAGINGQUERIES: 'stagingqueries'
} as const;

export type EntityId = (typeof EntityTypes)[keyof typeof EntityTypes];

export const entityConfig = [
	{
		label: 'Models',
		path: '/models',
		icon: IconCube,
		id: EntityTypes.MODELS,
		type: ConfType.MODEL
	},
	{
		label: 'Joins',
		path: '/joins',
		icon: IconSquare3Stack3d,
		id: EntityTypes.JOINS,
		type: ConfType.JOIN
	},
	{
		label: 'GroupBys',
		path: '/groupbys',
		icon: IconRectangleStack,
		id: EntityTypes.GROUPBYS,
		type: ConfType.GROUP_BY
	},
	{
		label: 'Staging Queries',
		path: '/stagingqueries',
		icon: IconCubeTransparent,
		id: EntityTypes.STAGINGQUERIES,
		type: ConfType.STAGING_QUERY
	}
] as const;

export type Entity = (typeof entityConfig)[number];

// This is a workaround, see https://app.asana.com/0/1208277377735902/1209205208293672/f
export type EntityWithType = (IJoinArgs | IGroupByArgs | IModelArgs | IStagingQueryArgs) & {
	entityType: EntityId;
};

// Helper function to get entity config by ID
export function getEntity(id: EntityId): Entity {
	const entity = entityConfig.find((entity) => entity.id === id);
	if (!entity) throw new Error(`Entity with id "${id}" not found`);
	return entity;
}
