import {
	type IEntitySourceArgs,
	type IEventSourceArgs,
	type ISourceArgs
} from '$lib/types/codegen';
import { type EntityData } from '$src/lib/types/Entity';

/**
 * Returns true if the node is a streaming node
 */
export function isStreaming(entity: EntityData | IEntitySourceArgs | IEventSourceArgs): boolean {
	if ('topic' in entity || 'mutationTopic' in entity) {
		return true;
	} else if ('table' in entity || 'snapshotTable' in entity) {
		// Source without topic
		return false;
	} else if ('left' in entity && entity.left) {
		// Join
		return isSourceStreaming(entity.left);
	} else if ('sources' in entity) {
		// Check if any upstream sources are streaming
		return entity.sources?.some((source) => isSourceStreaming(source)) ?? false;
	} else if ('join' in entity) {
		// Check if any upstream joins are streaming
		return isStreaming(entity.join!);
	} else if ('joinParts' in entity) {
		// Check if any upstream joinParts are streaming
		return entity.joinParts?.some((joinPart) => isStreaming(joinPart.groupBy!)) ?? true;
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
