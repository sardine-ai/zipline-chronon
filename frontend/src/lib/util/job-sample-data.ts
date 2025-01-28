import {
	statusColors,
	type JobStatus,
	type JobRunList,
	type JobDependencyGraph,
	type JobTreeNode
} from '$lib/types/Job/Job';

export function generateJobRunListSampleData(
	numJobs: number = 2,
	daysPerJob: number = 15
): JobRunList {
	const statuses = Object.keys(statusColors) as JobStatus[];
	const jobRunList: JobRunList = {};

	// For each job
	for (let jobId = 1; jobId <= numJobs; jobId++) {
		const jobKey = `job_${jobId}`;
		jobRunList[jobKey] = [];
		let currentDate = new Date('2024-01-01');
		let remainingDays = daysPerJob;

		// Generate multiple status periods for each job
		while (remainingDays > 0) {
			const duration = Math.min(Math.floor(Math.random() * 6) + 2, remainingDays);
			const endDate = new Date(currentDate);
			endDate.setDate(endDate.getDate() + duration - 1);

			jobRunList[jobKey].push({
				start: currentDate.toISOString().split('T')[0],
				end: endDate.toISOString().split('T')[0],
				status: statuses[Math.floor(Math.random() * statuses.length)]
			});

			currentDate = new Date(endDate);
			currentDate.setDate(currentDate.getDate() + 1);
			remainingDays -= duration;
		}
	}

	return jobRunList;
}

export function generateJobDependencyGraphSampleData(jobRunList: JobRunList): JobDependencyGraph {
	const uniqueJobs = Object.keys(jobRunList);
	const adjacencyList: Record<string, string[]> = {};

	// Create root nodes (roughly 1/3 of the jobs as root nodes)
	const numRootJobs = Math.max(1, Math.floor(uniqueJobs.length / 3));
	const rootNodes = uniqueJobs.slice(0, numRootJobs);
	const remainingJobs = uniqueJobs.slice(numRootJobs);

	// For each potential parent node
	const allPotentialParents = [...rootNodes];
	while (remainingJobs.length > 0 && allPotentialParents.length > 0) {
		// Pick a random parent from the potential parents
		const parentIndex = Math.floor(Math.random() * allPotentialParents.length);
		const parent = allPotentialParents[parentIndex];

		// 70% chance to create children for this parent
		if (Math.random() < 0.7 && remainingJobs.length > 0) {
			// Create 1-2 children for this parent
			const numChildren = Math.min(Math.floor(Math.random() * 2) + 1, remainingJobs.length);
			const children = remainingJobs.splice(0, numChildren);

			// Add to adjacency list
			adjacencyList[parent] = children;

			// Add these children as potential parents for the next iteration
			allPotentialParents.push(...children);
		}

		// Remove this parent from potential parents list
		allPotentialParents.splice(parentIndex, 1);
	}

	// Add any remaining jobs as root nodes
	rootNodes.push(...remainingJobs);

	return {
		adjacencyList,
		rootNodes
	};
}

export const jobRunListExample: JobRunList = {
	job_1: [
		{
			start: '2024-01-01',
			end: '2024-01-03',
			status: 'invalid'
		},
		{
			start: '2024-01-04',
			end: '2024-01-10',
			status: 'running'
		},
		{
			start: '2024-01-11',
			end: '2024-01-17',
			status: 'failed'
		},
		{
			start: '2024-01-18',
			end: '2024-01-21',
			status: 'completed'
		}
	],
	job_2: [
		{
			start: '2024-01-01',
			end: '2024-01-04',
			status: 'completed'
		},
		{
			start: '2024-01-05',
			end: '2024-01-07',
			status: 'running'
		},
		{
			start: '2024-01-08',
			end: '2024-01-10',
			status: 'completed'
		},
		{
			start: '2024-01-11',
			end: '2024-01-17',
			status: 'queued'
		},
		{
			start: '2024-01-18',
			end: '2024-01-21',
			status: 'waiting'
		}
	],
	job_3: [
		{
			start: '2024-01-01',
			end: '2024-01-05',
			status: 'queued'
		},
		{
			start: '2024-01-06',
			end: '2024-01-08',
			status: 'queued'
		},
		{
			start: '2024-01-09',
			end: '2024-01-15',
			status: 'completed'
		},
		{
			start: '2024-01-16',
			end: '2024-01-20',
			status: 'completed'
		},
		{
			start: '2024-01-21',
			end: '2024-01-21',
			status: 'completed'
		}
	],
	job_4: [
		{
			start: '2024-01-01',
			end: '2024-01-06',
			status: 'invalid'
		},
		{
			start: '2024-01-07',
			end: '2024-01-11',
			status: 'completed'
		},
		{
			start: '2024-01-12',
			end: '2024-01-18',
			status: 'running'
		},
		{
			start: '2024-01-19',
			end: '2024-01-20',
			status: 'completed'
		},
		{
			start: '2024-01-21',
			end: '2024-01-21',
			status: 'running'
		}
	],
	job_5: [
		{
			start: '2024-01-01',
			end: '2024-01-04',
			status: 'running'
		},
		{
			start: '2024-01-05',
			end: '2024-01-11',
			status: 'queued'
		},
		{
			start: '2024-01-12',
			end: '2024-01-16',
			status: 'running'
		},
		{
			start: '2024-01-17',
			end: '2024-01-21',
			status: 'queued'
		}
	],
	job_6: [
		{
			start: '2024-01-01',
			end: '2024-01-06',
			status: 'completed'
		},
		{
			start: '2024-01-07',
			end: '2024-01-11',
			status: 'completed'
		},
		{
			start: '2024-01-12',
			end: '2024-01-16',
			status: 'waiting'
		},
		{
			start: '2024-01-17',
			end: '2024-01-20',
			status: 'completed'
		},
		{
			start: '2024-01-21',
			end: '2024-01-21',
			status: 'running'
		}
	],
	job_7: [
		{
			start: '2024-01-01',
			end: '2024-01-06',
			status: 'completed'
		},
		{
			start: '2024-01-07',
			end: '2024-01-13',
			status: 'running'
		},
		{
			start: '2024-01-14',
			end: '2024-01-15',
			status: 'waiting'
		},
		{
			start: '2024-01-16',
			end: '2024-01-21',
			status: 'queued'
		}
	],
	job_8: [
		{
			start: '2024-01-01',
			end: '2024-01-04',
			status: 'completed'
		},
		{
			start: '2024-01-05',
			end: '2024-01-09',
			status: 'running'
		},
		{
			start: '2024-01-10',
			end: '2024-01-11',
			status: 'invalid'
		},
		{
			start: '2024-01-12',
			end: '2024-01-15',
			status: 'completed'
		},
		{
			start: '2024-01-16',
			end: '2024-01-18',
			status: 'queued'
		},
		{
			start: '2024-01-19',
			end: '2024-01-21',
			status: 'invalid'
		}
	],
	job_9: [
		{
			start: '2024-01-01',
			end: '2024-01-02',
			status: 'invalid'
		},
		{
			start: '2024-01-03',
			end: '2024-01-09',
			status: 'failed'
		},
		{
			start: '2024-01-10',
			end: '2024-01-12',
			status: 'running'
		},
		{
			start: '2024-01-13',
			end: '2024-01-17',
			status: 'queued'
		},
		{
			start: '2024-01-18',
			end: '2024-01-21',
			status: 'failed'
		}
	],
	job_10: [
		{
			start: '2024-01-01',
			end: '2024-01-05',
			status: 'invalid'
		},
		{
			start: '2024-01-06',
			end: '2024-01-12',
			status: 'running'
		},
		{
			start: '2024-01-13',
			end: '2024-01-18',
			status: 'queued'
		},
		{
			start: '2024-01-19',
			end: '2024-01-21',
			status: 'failed'
		}
	]
};

export const jobDependencyGraphExample: JobDependencyGraph = {
	adjacencyList: {
		job_1: ['job_4'],
		job_4: ['job_5', 'job_6'],
		job_6: ['job_7', 'job_9'],
		job_7: ['job_8'],
		job_9: ['job_10']
	},
	rootNodes: ['job_1', 'job_2', 'job_3']
};

export const jobTreeNodeExample: JobTreeNode[] = [
	{
		row: 'job_1',
		runs: [
			{
				start: '2024-01-01',
				end: '2024-01-03',
				status: 'invalid'
			},
			{
				start: '2024-01-04',
				end: '2024-01-10',
				status: 'running'
			},
			{
				start: '2024-01-11',
				end: '2024-01-17',
				status: 'failed'
			},
			{
				start: '2024-01-18',
				end: '2024-01-21',
				status: 'completed'
			}
		],
		children: [
			{
				row: 'job_4',
				runs: [
					{
						start: '2024-01-01',
						end: '2024-01-06',
						status: 'invalid'
					},
					{
						start: '2024-01-07',
						end: '2024-01-11',
						status: 'completed'
					},
					{
						start: '2024-01-12',
						end: '2024-01-18',
						status: 'running'
					},
					{
						start: '2024-01-19',
						end: '2024-01-20',
						status: 'completed'
					},
					{
						start: '2024-01-21',
						end: '2024-01-21',
						status: 'running'
					}
				],
				children: [
					{
						row: 'job_5',
						runs: [
							{
								start: '2024-01-01',
								end: '2024-01-04',
								status: 'running'
							},
							{
								start: '2024-01-05',
								end: '2024-01-11',
								status: 'queued'
							},
							{
								start: '2024-01-12',
								end: '2024-01-16',
								status: 'running'
							},
							{
								start: '2024-01-17',
								end: '2024-01-21',
								status: 'queued'
							}
						],
						children: []
					},
					{
						row: 'job_6',
						runs: [
							{
								start: '2024-01-01',
								end: '2024-01-06',
								status: 'completed'
							},
							{
								start: '2024-01-07',
								end: '2024-01-11',
								status: 'completed'
							},
							{
								start: '2024-01-12',
								end: '2024-01-16',
								status: 'waiting'
							},
							{
								start: '2024-01-17',
								end: '2024-01-20',
								status: 'completed'
							},
							{
								start: '2024-01-21',
								end: '2024-01-21',
								status: 'running'
							}
						],
						children: [
							{
								row: 'job_7',
								runs: [
									{
										start: '2024-01-01',
										end: '2024-01-06',
										status: 'completed'
									},
									{
										start: '2024-01-07',
										end: '2024-01-13',
										status: 'running'
									},
									{
										start: '2024-01-14',
										end: '2024-01-15',
										status: 'waiting'
									},
									{
										start: '2024-01-16',
										end: '2024-01-21',
										status: 'queued'
									}
								],
								children: [
									{
										row: 'job_8',
										runs: [
											{
												start: '2024-01-01',
												end: '2024-01-04',
												status: 'completed'
											},
											{
												start: '2024-01-05',
												end: '2024-01-09',
												status: 'running'
											},
											{
												start: '2024-01-10',
												end: '2024-01-11',
												status: 'invalid'
											},
											{
												start: '2024-01-12',
												end: '2024-01-15',
												status: 'completed'
											},
											{
												start: '2024-01-16',
												end: '2024-01-18',
												status: 'queued'
											},
											{
												start: '2024-01-19',
												end: '2024-01-21',
												status: 'invalid'
											}
										],
										children: []
									}
								]
							},
							{
								row: 'job_9',
								runs: [
									{
										start: '2024-01-01',
										end: '2024-01-02',
										status: 'invalid'
									},
									{
										start: '2024-01-03',
										end: '2024-01-09',
										status: 'failed'
									},
									{
										start: '2024-01-10',
										end: '2024-01-12',
										status: 'running'
									},
									{
										start: '2024-01-13',
										end: '2024-01-17',
										status: 'queued'
									},
									{
										start: '2024-01-18',
										end: '2024-01-21',
										status: 'failed'
									}
								],
								children: [
									{
										row: 'job_10',
										runs: [
											{
												start: '2024-01-01',
												end: '2024-01-05',
												status: 'invalid'
											},
											{
												start: '2024-01-06',
												end: '2024-01-12',
												status: 'running'
											},
											{
												start: '2024-01-13',
												end: '2024-01-18',
												status: 'queued'
											},
											{
												start: '2024-01-19',
												end: '2024-01-21',
												status: 'failed'
											}
										],
										children: []
									}
								]
							}
						]
					}
				]
			}
		]
	},
	{
		row: 'job_2',
		runs: [
			{
				start: '2024-01-01',
				end: '2024-01-04',
				status: 'completed'
			},
			{
				start: '2024-01-05',
				end: '2024-01-07',
				status: 'running'
			},
			{
				start: '2024-01-08',
				end: '2024-01-10',
				status: 'completed'
			},
			{
				start: '2024-01-11',
				end: '2024-01-17',
				status: 'queued'
			},
			{
				start: '2024-01-18',
				end: '2024-01-21',
				status: 'waiting'
			}
		],
		children: []
	},
	{
		row: 'job_3',
		runs: [
			{
				start: '2024-01-01',
				end: '2024-01-05',
				status: 'queued'
			},
			{
				start: '2024-01-06',
				end: '2024-01-08',
				status: 'queued'
			},
			{
				start: '2024-01-09',
				end: '2024-01-15',
				status: 'completed'
			},
			{
				start: '2024-01-16',
				end: '2024-01-20',
				status: 'completed'
			},
			{
				start: '2024-01-21',
				end: '2024-01-21',
				status: 'completed'
			}
		],
		children: []
	}
];
