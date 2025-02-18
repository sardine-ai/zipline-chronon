import { describe, it, expect } from 'vitest';
import { buildJobTrackerTree, organizeTasksByDate } from '$lib/job/tree-builder/tree-builder';
import { Status, type IJobTrackerResponseArgs, type ILineageResponse } from '$lib/types/codegen';

describe('jobTracker.service', () => {
	describe('buildJobTrackerTree', () => {
		it('should handle empty lineage', () => {
			const emptyLineage: ILineageResponse = {
				nodeGraph: {
					connections: new Map()
				}
			};
			const result = buildJobTrackerTree(emptyLineage, new Map());
			expect(result.jobTree).toEqual([]);
			expect(result.dates).toEqual([]);
		});

		it('should handle overlapping task dates correctly', () => {
			const lineage: ILineageResponse = {
				nodeGraph: {
					connections: new Map([[{ name: 'job1' }, { parents: [] }]])
				}
			};

			const jobTrackerData = new Map<string, IJobTrackerResponseArgs>([
				[
					'job1',
					{
						tasks: [
							{
								dateRange: {
									startDate: '2024-01-01',
									endDate: '2024-01-03'
								},
								submittedTs: 1000,
								status: Status.FAILED
							},
							{
								dateRange: {
									startDate: '2024-01-02',
									endDate: '2024-01-04'
								},
								submittedTs: 2000,
								status: Status.SUCCESS
							}
						]
					}
				]
			]);

			const result = buildJobTrackerTree(lineage, jobTrackerData);

			const tasksByDate = result.jobTree[0].jobTracker?.tasksByDate ?? [];
			expect(tasksByDate).toHaveLength(2);

			// Jan 1 (FAILED)
			expect(tasksByDate[0]).toMatchObject({
				dateRange: { startDate: '2024-01-01', endDate: '2024-01-01' },
				status: Status.FAILED
			});

			// Jan 2-4 (SUCCESS)
			expect(tasksByDate[1]).toMatchObject({
				dateRange: { startDate: '2024-01-02', endDate: '2024-01-04' },
				status: Status.SUCCESS
			});
		});

		it('should build hierarchical tree structure', () => {
			const lineage: ILineageResponse = {
				nodeGraph: {
					connections: new Map([
						[
							{ name: 'child' },
							{
								parents: [{ name: 'parent' }]
							}
						],
						[
							{ name: 'parent' },
							{
								parents: []
							}
						]
					])
				}
			};

			const jobTrackerData = new Map<string, IJobTrackerResponseArgs>([
				[
					'child',
					{
						tasks: [
							{
								dateRange: {
									startDate: '2024-01-01',
									endDate: '2024-01-01'
								},
								status: Status.SUCCESS
							}
						]
					}
				],
				[
					'parent',
					{
						tasks: [
							{
								dateRange: {
									startDate: '2024-01-01',
									endDate: '2024-01-01'
								},
								status: Status.SUCCESS
							}
						]
					}
				]
			]);

			const result = buildJobTrackerTree(lineage, jobTrackerData);

			expect(result.jobTree).toHaveLength(1);
			expect(result.jobTree[0].row).toBe('child');
			expect(result.jobTree[0].children).toHaveLength(1);
			expect(result.jobTree[0].children[0].row).toBe('parent');
		});

		it('should throw error when lineage is undefined', () => {
			expect(() =>
				buildJobTrackerTree(undefined as unknown as ILineageResponse, new Map())
			).toThrow('Lineage data is required for job tracking');
		});
	});

	describe('organizeTasksByDate', () => {
		it('should handle empty tasks array', () => {
			expect(organizeTasksByDate([])).toEqual([]);
		});

		it('should handle single task', () => {
			const task = {
				dateRange: { startDate: '2024-01-01', endDate: '2024-01-03' },
				submittedTs: 1000,
				status: Status.SUCCESS
			};
			expect(organizeTasksByDate([task])).toEqual([task]);
		});

		it('should handle non-overlapping tasks', () => {
			const tasks = [
				{
					dateRange: { startDate: '2024-01-01', endDate: '2024-01-02' },
					submittedTs: 1000,
					status: Status.SUCCESS
				},
				{
					dateRange: { startDate: '2024-01-04', endDate: '2024-01-05' },
					submittedTs: 2000,
					status: Status.FAILED
				}
			];
			expect(organizeTasksByDate(tasks)).toEqual(tasks);
		});

		it('should handle complete overlap with newer task', () => {
			const tasks = [
				{
					dateRange: { startDate: '2024-01-01', endDate: '2024-01-03' },
					submittedTs: 1000,
					status: Status.FAILED
				},
				{
					dateRange: { startDate: '2024-01-01', endDate: '2024-01-03' },
					submittedTs: 2000,
					status: Status.SUCCESS
				}
			];
			expect(organizeTasksByDate(tasks)).toEqual([tasks[1]]);
		});

		it('should handle partial overlap with newer task', () => {
			const tasks = [
				{
					dateRange: { startDate: '2024-01-01', endDate: '2024-01-03' },
					submittedTs: 1000,
					status: Status.FAILED
				},
				{
					dateRange: { startDate: '2024-01-02', endDate: '2024-01-04' },
					submittedTs: 2000,
					status: Status.SUCCESS
				}
			];
			const expected = [
				{
					dateRange: { startDate: '2024-01-01', endDate: '2024-01-01' },
					submittedTs: 1000,
					status: Status.FAILED
				},
				{
					dateRange: { startDate: '2024-01-02', endDate: '2024-01-04' },
					submittedTs: 2000,
					status: Status.SUCCESS
				}
			];
			expect(organizeTasksByDate(tasks)).toEqual(expected);
		});

		it('should handle multiple overlapping tasks', () => {
			const tasks = [
				{
					dateRange: { startDate: '2024-01-01', endDate: '2024-01-05' },
					submittedTs: 1000,
					status: Status.RUNNING
				},
				{
					dateRange: { startDate: '2024-01-02', endDate: '2024-01-04' },
					submittedTs: 2000,
					status: Status.FAILED
				},
				{
					dateRange: { startDate: '2024-01-03', endDate: '2024-01-06' },
					submittedTs: 3000,
					status: Status.SUCCESS
				}
			];
			const expected = [
				{
					dateRange: { startDate: '2024-01-01', endDate: '2024-01-01' },
					submittedTs: 1000,
					status: Status.RUNNING
				},
				{
					dateRange: { startDate: '2024-01-02', endDate: '2024-01-02' },
					submittedTs: 2000,
					status: Status.FAILED
				},
				{
					dateRange: { startDate: '2024-01-03', endDate: '2024-01-06' },
					submittedTs: 3000,
					status: Status.SUCCESS
				}
			];
			expect(organizeTasksByDate(tasks)).toEqual(expected);
		});

		it('should handle tasks with missing date ranges', () => {
			const tasks = [
				{
					submittedTs: 1000,
					status: Status.FAILED
				},
				{
					dateRange: { startDate: '2024-01-01', endDate: '2024-01-03' },
					submittedTs: 2000,
					status: Status.SUCCESS
				}
			];
			expect(organizeTasksByDate(tasks)).toEqual([tasks[1]]);
		});
	});
});
