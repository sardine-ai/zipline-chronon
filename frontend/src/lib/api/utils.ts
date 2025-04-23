import { InternMap } from 'd3';
import {
	LogicalType,
	type IJoinArgs,
	type IJoinPartArgs,
	type ILineageResponseArgs,
	type ILogicalNodeArgs,
	type INodeKeyArgs,
	type ISourceArgs,
	type INodeGraphArgs
} from '$lib/types/codegen';
import { getEntityConfig, type EntityData } from '../types/Entity';

/** Convert Join to LineageResponse by walking joinParts */
export function confToLineage(conf: EntityData, excludeLeft = false): ILineageResponseArgs {
	// Use `InternMap` insteaad of `Map` to support object keys (instances will be different once serialized/fetched from API) - https://d3js.org/d3-array/intern
	// @ts-expect-error: Bad typing
	const connections: NodeGraph['connections'] = new InternMap([], JSON.stringify);
	// @ts-expect-error: Bad typing
	const infoMap: NodeGraph['infoMap'] = new InternMap([], JSON.stringify);

	const logicalType = getEntityConfig(conf).logicalType;

	const confNodeKey: INodeKeyArgs = {
		name:
			'metaData' in conf && conf.metaData
				? conf.metaData.name
				: 'table' in conf
					? conf.table
					: 'Unknown',
		logicalType
	};
	infoMap.set(confNodeKey, {
		conf: conf as ILogicalNodeArgs
	});
	const confParents: INodeKeyArgs[] = [];
	connections.set(confNodeKey, { parents: confParents, children: [] });

	/*
	 * Join
	 */
	if ('left' in conf && conf.left && !excludeLeft) {
		processSource(conf.left, infoMap, connections, confParents, confNodeKey);
	}

	if ('joinParts' in conf && conf.joinParts) {
		processJoinParts(conf.joinParts, infoMap, connections, confParents, confNodeKey);
	}

	/*
	 * GroupBy
	 */
	if ('sources' in conf && conf.sources) {
		for (const source of conf.sources ?? []) {
			processSource(source, infoMap, connections, confParents, confNodeKey);
		}
	}

	/*
	 * Model
	 */
	if ('source' in conf && conf.source) {
		processSource(conf.source, infoMap, connections, confParents, confNodeKey);
	}

	return {
		nodeGraph: {
			connections,
			infoMap
		},
		mainNode: confNodeKey
	};
}

function processJoinParts(
	joinParts: IJoinPartArgs[],
	infoMap: NonNullable<INodeGraphArgs['infoMap']>,
	connections: NonNullable<INodeGraphArgs['connections']>,
	parents: INodeKeyArgs[],
	parentNode: INodeKeyArgs
) {
	for (const jp of joinParts ?? []) {
		if (jp.groupBy) {
			const groupByNodeKey: INodeKeyArgs = {
				name: jp.groupBy.metaData?.name,
				logicalType: LogicalType.GROUP_BY
			};
			infoMap.set(groupByNodeKey, {
				conf: jp.groupBy as ILogicalNodeArgs
			});
			parents.push(groupByNodeKey);

			const groupByParents: INodeKeyArgs[] = [];
			connections.set(groupByNodeKey, { parents: groupByParents, children: [parentNode] });

			for (const source of jp.groupBy?.sources ?? []) {
				processSource(source, infoMap, connections, groupByParents, groupByNodeKey);
			}
		}
	}
}

function processSource(
	source: ISourceArgs,
	infoMap: NonNullable<INodeGraphArgs['infoMap']>,
	connections: NonNullable<INodeGraphArgs['connections']>,
	parents: INodeKeyArgs[],
	parentNode: INodeKeyArgs
) {
	if (source.entities) {
		const entityNodeKey: INodeKeyArgs = {
			name: source.entities.snapshotTable,
			logicalType: LogicalType.TABULAR_DATA
		};
		infoMap.set(entityNodeKey, {
			conf: source.entities as ILogicalNodeArgs
		});
		parents.push(entityNodeKey);
		connections.set(entityNodeKey, { parents: [], children: [parentNode] });
	}

	if (source.events) {
		const eventNodeKey: INodeKeyArgs = {
			name: source.events.table,
			logicalType: LogicalType.TABULAR_DATA
		};
		infoMap.set(eventNodeKey, {
			conf: source.events as ILogicalNodeArgs
		});
		parents.push(eventNodeKey);
		connections.set(eventNodeKey, { parents: [], children: [parentNode] });
	}

	if (source.joinSource) {
		const joinNodeKey: INodeKeyArgs = {
			name: source.joinSource.join?.metaData?.name,
			logicalType: LogicalType.TABULAR_DATA
		};
		infoMap.set(joinNodeKey, {
			conf: source.joinSource as ILogicalNodeArgs
		});
		parents.push(joinNodeKey);
		connections.set(joinNodeKey, { parents: [], children: [parentNode] });

		// Transfer connections and infoMap from joinSource join to root join graph
		const joinSourceLineage = confToLineage(source.joinSource.join as IJoinArgs);

		for (const [key, nodeConnections] of joinSourceLineage.nodeGraph?.connections ?? []) {
			const newKey = key === joinSourceLineage.mainNode ? joinNodeKey : key;
			const existingConnections = connections.get(newKey) || { parents: [], children: [] };
			connections.set(newKey, {
				parents: [...(existingConnections.parents ?? []), ...(nodeConnections.parents || [])],
				children: [...(existingConnections.children ?? []), ...(nodeConnections.children || [])]
			});
		}

		for (const [key, info] of joinSourceLineage.nodeGraph?.infoMap ?? []) {
			infoMap.set(key, info);
		}
	}
}
