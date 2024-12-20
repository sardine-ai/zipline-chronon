import IconCube from '~icons/heroicons/cube-16-solid';
import IconPuzzlePiece from '~icons/heroicons/puzzle-piece-16-solid';
import IconSquare3Stack3d from '~icons/heroicons/square-3-stack-3d-16-solid';

export const entityConfig = [
	{
		label: 'Models',
		path: '/models',
		icon: IconCube,
		id: 'models'
	},
	{
		label: 'GroupBys',
		path: '/groupbys',
		icon: IconPuzzlePiece,
		id: 'groupbys'
	},
	{
		label: 'Joins',
		path: '/joins',
		icon: IconSquare3Stack3d,
		id: 'joins'
	}
] as const;

export type Entity = (typeof entityConfig)[number];
export type EntityId = Entity['id'];

// Helper function to get entity by ID
export function getEntity(id: EntityId): Entity {
	const entity = entityConfig.find((entity) => entity.id === id);
	if (!entity) throw new Error(`Entity with id "${id}" not found`);
	return entity;
}
