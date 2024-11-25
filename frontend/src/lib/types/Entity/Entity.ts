import { Cube, PuzzlePiece, Square3Stack3d } from 'svelte-hero-icons';

export const entityConfig = [
	{
		label: 'Models',
		path: '/models',
		icon: Cube,
		id: 'models'
	},
	{
		label: 'GroupBys',
		path: '/groupbys',
		icon: PuzzlePiece,
		id: 'groupbys'
	},
	{
		label: 'Joins',
		path: '/joins',
		icon: Square3Stack3d,
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
