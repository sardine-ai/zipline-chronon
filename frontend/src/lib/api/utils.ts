import { InternMap } from 'd3';
import {
	LogicalType,
	type IJoin,
	type IJoinPart,
	type ILineageResponse,
	type ILogicalNode,
	type INodeKey,
	type ISource,
	type NodeGraph
} from '../types/codegen';
import { getLogicalNodeType, type CombinedLogicalNode } from '../types/LogicalNode';

/** Convert Join to LineageResponse by walking joinParts */
export function confToLineage(conf: CombinedLogicalNode, excludeLeft = false): ILineageResponse {
	// Use `InternMap` insteaad of `Map` to support object keys (instances will be different once serialized/fetched from API) - https://d3js.org/d3-array/intern
	// @ts-expect-error: Bad typing
	const connections: NodeGraph['connections'] = new InternMap([], JSON.stringify);
	// @ts-expect-error: Bad typing
	const infoMap: NodeGraph['infoMap'] = new InternMap([], JSON.stringify);

	const logicalType = getLogicalNodeType(conf);

	const confNodeKey: INodeKey = {
		name:
			'metaData' in conf && conf.metaData
				? conf.metaData.name
				: 'table' in conf
					? conf.table
					: 'Unknown',
		logicalType
	};
	infoMap.set(confNodeKey, {
		conf: conf as ILogicalNode
	});
	const confParents: INodeKey[] = [];
	connections.set(confNodeKey, { parents: confParents });

	/*
	 * Join
	 */
	if ('left' in conf && conf.left && !excludeLeft) {
		processSource(conf.left, infoMap, connections, confParents);
	}

	if ('joinParts' in conf && conf.joinParts) {
		processJoinParts(conf.joinParts, infoMap, connections, confParents);
	}

	/*
	 * GroupBy
	 */
	if ('sources' in conf && conf.sources) {
		for (const source of conf.sources ?? []) {
			processSource(source, infoMap, connections, confParents);
		}
	}

	/*
	 * Model
	 */
	if ('source' in conf && conf.source) {
		processSource(conf.source, infoMap, connections, confParents);
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
	joinParts: IJoinPart[],
	infoMap: NonNullable<NodeGraph['infoMap']>,
	connections: NonNullable<NodeGraph['connections']>,
	parents: INodeKey[]
) {
	for (const jp of joinParts ?? []) {
		if (jp.groupBy) {
			const groupByNodeKey: INodeKey = {
				name: jp.groupBy.metaData?.name,
				logicalType: LogicalType.GROUP_BY
			};
			infoMap.set(groupByNodeKey, {
				conf: jp.groupBy as ILogicalNode
			});
			parents.push(groupByNodeKey);

			const groupByParents: INodeKey[] = [];
			connections.set(groupByNodeKey, { parents: groupByParents });

			for (const source of jp.groupBy?.sources ?? []) {
				processSource(source, infoMap, connections, groupByParents);
			}
		}
	}
}

function processSource(
	source: ISource,
	infoMap: NonNullable<NodeGraph['infoMap']>,
	connections: NonNullable<NodeGraph['connections']>,
	parents: INodeKey[]
) {
	if (source.entities) {
		const entityNodeKey: INodeKey = {
			name: source.entities.snapshotTable,
			logicalType: LogicalType.TABULAR_DATA // TODO: Are all sources tabular data?
		};
		infoMap.set(entityNodeKey, {
			conf: source.entities as ILogicalNode
		});
		parents.push(entityNodeKey);
	}

	if (source.events) {
		const eventNodeKey: INodeKey = {
			name: source.events.table,
			logicalType: LogicalType.TABULAR_DATA // TODO: Are all sources tabular data?
		};
		infoMap.set(eventNodeKey, {
			conf: source.events as ILogicalNode
		});
		parents.push(eventNodeKey);
	}

	if (source.joinSource) {
		const joinNodeKey: INodeKey = {
			name: source.joinSource.join?.metaData?.name,
			logicalType: LogicalType.TABULAR_DATA // TODO: Are all sources tabular data?
		};
		infoMap.set(joinNodeKey, {
			conf: source.joinSource as ILogicalNode
		});
		parents.push(joinNodeKey);

		// Transfer connections and infoMap from joinSource join to root join graph
		const joinSourceLineage = confToLineage(source.joinSource.join as IJoin);

		for (const [key, nodeConnections] of joinSourceLineage.nodeGraph?.connections ?? []) {
			connections.set(key === joinSourceLineage.mainNode ? joinNodeKey : key, nodeConnections);
		}

		for (const [key, info] of joinSourceLineage.nodeGraph?.infoMap ?? []) {
			infoMap.set(key, info);
		}
	}
}
