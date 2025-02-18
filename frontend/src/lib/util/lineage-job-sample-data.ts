import {
	LogicalType,
	Status,
	type ILineageResponseArgs,
	type IJobTrackerResponseArgs
} from '$lib/types/codegen';

export const sampleLineageResponse: ILineageResponseArgs = {
	nodeGraph: {
		connections: new Map([
			[
				{ name: 'job_1', logicalType: LogicalType.JOIN },
				{
					parents: [],
					children: [{ name: 'job_4', logicalType: LogicalType.JOIN }]
				}
			],
			[
				{ name: 'job_4', logicalType: LogicalType.JOIN },
				{
					parents: [{ name: 'job_1', logicalType: LogicalType.JOIN }],
					children: [
						{ name: 'job_5', logicalType: LogicalType.JOIN },
						{ name: 'job_6', logicalType: LogicalType.JOIN }
					]
				}
			],
			[
				{ name: 'job_5', logicalType: LogicalType.JOIN },
				{
					parents: [{ name: 'job_4', logicalType: LogicalType.JOIN }],
					children: []
				}
			],
			[
				{ name: 'job_6', logicalType: LogicalType.JOIN },
				{
					parents: [{ name: 'job_4', logicalType: LogicalType.JOIN }],
					children: [
						{ name: 'job_7', logicalType: LogicalType.JOIN },
						{ name: 'job_9', logicalType: LogicalType.JOIN }
					]
				}
			],
			[
				{ name: 'job_7', logicalType: LogicalType.JOIN },
				{
					parents: [{ name: 'job_6', logicalType: LogicalType.JOIN }],
					children: [{ name: 'job_8', logicalType: LogicalType.JOIN }]
				}
			],
			[
				{ name: 'job_8', logicalType: LogicalType.JOIN },
				{
					parents: [{ name: 'job_7', logicalType: LogicalType.JOIN }],
					children: []
				}
			],
			[
				{ name: 'job_9', logicalType: LogicalType.JOIN },
				{
					parents: [{ name: 'job_6', logicalType: LogicalType.JOIN }],
					children: [{ name: 'job_10', logicalType: LogicalType.JOIN }]
				}
			],
			[
				{ name: 'job_10', logicalType: LogicalType.JOIN },
				{
					parents: [{ name: 'job_9', logicalType: LogicalType.JOIN }],
					children: []
				}
			],
			[
				{ name: 'job_2', logicalType: LogicalType.JOIN },
				{
					parents: [],
					children: []
				}
			],
			[
				{ name: 'job_3', logicalType: LogicalType.JOIN },
				{
					parents: [],
					children: []
				}
			]
		]),
		infoMap: new Map()
	},
	mainNode: {
		name: 'job_1',
		logicalType: LogicalType.JOIN
	}
};

export const job1Response: IJobTrackerResponseArgs = {
	tasks: [
		{
			status: Status.SUCCESS,
			submittedTs: new Date('2024-01-18').getTime(),
			dateRange: {
				startDate: '2024-01-18',
				endDate: '2024-01-21'
			}
		},
		{
			status: Status.FAILED,
			submittedTs: new Date('2024-01-11').getTime(),
			dateRange: {
				startDate: '2024-01-11',
				endDate: '2024-01-17'
			}
		},
		{
			status: Status.RUNNING,
			submittedTs: new Date('2024-01-04').getTime(),
			dateRange: {
				startDate: '2024-01-04',
				endDate: '2024-01-10'
			}
		},
		{
			status: Status.UPSTREAM_FAILED,
			submittedTs: new Date('2024-01-01').getTime(),
			dateRange: {
				startDate: '2024-01-01',
				endDate: '2024-01-03'
			}
		}
	],
	mainNode: {
		name: 'job_1',
		logicalType: LogicalType.JOIN
	}
};

export const job2Response: IJobTrackerResponseArgs = {
	tasks: [
		{
			status: Status.WAITING_FOR_RESOURCES,
			submittedTs: new Date('2024-01-18').getTime(),
			dateRange: {
				startDate: '2024-01-18',
				endDate: '2024-01-21'
			}
		},
		{
			status: Status.QUEUED,
			submittedTs: new Date('2024-01-11').getTime(),
			dateRange: {
				startDate: '2024-01-11',
				endDate: '2024-01-17'
			}
		},
		{
			status: Status.SUCCESS,
			submittedTs: new Date('2024-01-08').getTime(),
			dateRange: {
				startDate: '2024-01-08',
				endDate: '2024-01-10'
			}
		},
		{
			status: Status.RUNNING,
			submittedTs: new Date('2024-01-05').getTime(),
			dateRange: {
				startDate: '2024-01-05',
				endDate: '2024-01-07'
			}
		},
		{
			status: Status.SUCCESS,
			submittedTs: new Date('2024-01-01').getTime(),
			dateRange: {
				startDate: '2024-01-01',
				endDate: '2024-01-04'
			}
		}
	],
	mainNode: {
		name: 'job_2',
		logicalType: LogicalType.JOIN
	}
};

export const job3Response: IJobTrackerResponseArgs = {
	tasks: [
		{
			status: Status.SUCCESS,
			submittedTs: new Date('2024-01-21').getTime(),
			dateRange: {
				startDate: '2024-01-21',
				endDate: '2024-01-21'
			}
		},
		{
			status: Status.SUCCESS,
			submittedTs: new Date('2024-01-16').getTime(),
			dateRange: {
				startDate: '2024-01-16',
				endDate: '2024-01-20'
			}
		},
		{
			status: Status.SUCCESS,
			submittedTs: new Date('2024-01-09').getTime(),
			dateRange: {
				startDate: '2024-01-09',
				endDate: '2024-01-15'
			}
		},
		{
			status: Status.QUEUED,
			submittedTs: new Date('2024-01-06').getTime(),
			dateRange: {
				startDate: '2024-01-06',
				endDate: '2024-01-08'
			}
		},
		{
			status: Status.QUEUED,
			submittedTs: new Date('2024-01-01').getTime(),
			dateRange: {
				startDate: '2024-01-01',
				endDate: '2024-01-05'
			}
		}
	],
	mainNode: {
		name: 'job_3',
		logicalType: LogicalType.JOIN
	}
};

export const job4Response: IJobTrackerResponseArgs = {
	tasks: [
		{
			status: Status.RUNNING,
			submittedTs: new Date('2024-01-21').getTime(),
			dateRange: {
				startDate: '2024-01-21',
				endDate: '2024-01-21'
			}
		},
		{
			status: Status.SUCCESS,
			submittedTs: new Date('2024-01-19').getTime(),
			dateRange: {
				startDate: '2024-01-19',
				endDate: '2024-01-20'
			}
		},
		{
			status: Status.RUNNING,
			submittedTs: new Date('2024-01-12').getTime(),
			dateRange: {
				startDate: '2024-01-12',
				endDate: '2024-01-18'
			}
		},
		{
			status: Status.SUCCESS,
			submittedTs: new Date('2024-01-07').getTime(),
			dateRange: {
				startDate: '2024-01-07',
				endDate: '2024-01-11'
			}
		},
		{
			status: Status.UPSTREAM_FAILED,
			submittedTs: new Date('2024-01-01').getTime(),
			dateRange: {
				startDate: '2024-01-01',
				endDate: '2024-01-06'
			}
		}
	],
	mainNode: {
		name: 'job_4',
		logicalType: LogicalType.JOIN
	}
};

export const job5Response: IJobTrackerResponseArgs = {
	tasks: [
		{
			status: Status.QUEUED,
			submittedTs: new Date('2024-01-17').getTime(),
			dateRange: {
				startDate: '2024-01-17',
				endDate: '2024-01-21'
			}
		},
		{
			status: Status.RUNNING,
			submittedTs: new Date('2024-01-12').getTime(),
			dateRange: {
				startDate: '2024-01-12',
				endDate: '2024-01-16'
			}
		},
		{
			status: Status.QUEUED,
			submittedTs: new Date('2024-01-05').getTime(),
			dateRange: {
				startDate: '2024-01-05',
				endDate: '2024-01-11'
			}
		},
		{
			status: Status.RUNNING,
			submittedTs: new Date('2024-01-01').getTime(),
			dateRange: {
				startDate: '2024-01-01',
				endDate: '2024-01-04'
			}
		}
	],
	mainNode: {
		name: 'job_5',
		logicalType: LogicalType.JOIN
	}
};

export const job6Response: IJobTrackerResponseArgs = {
	tasks: [
		{
			status: Status.RUNNING,
			submittedTs: new Date('2024-01-21').getTime(),
			dateRange: {
				startDate: '2024-01-21',
				endDate: '2024-01-21'
			}
		},
		{
			status: Status.SUCCESS,
			submittedTs: new Date('2024-01-17').getTime(),
			dateRange: {
				startDate: '2024-01-17',
				endDate: '2024-01-20'
			}
		},
		{
			status: Status.WAITING_FOR_RESOURCES,
			submittedTs: new Date('2024-01-12').getTime(),
			dateRange: {
				startDate: '2024-01-12',
				endDate: '2024-01-16'
			}
		},
		{
			status: Status.SUCCESS,
			submittedTs: new Date('2024-01-07').getTime(),
			dateRange: {
				startDate: '2024-01-07',
				endDate: '2024-01-11'
			}
		},
		{
			status: Status.SUCCESS,
			submittedTs: new Date('2024-01-01').getTime(),
			dateRange: {
				startDate: '2024-01-01',
				endDate: '2024-01-06'
			}
		}
	],
	mainNode: {
		name: 'job_6',
		logicalType: LogicalType.JOIN
	}
};

export const job7Response: IJobTrackerResponseArgs = {
	tasks: [
		{
			status: Status.QUEUED,
			submittedTs: new Date('2024-01-16').getTime(),
			dateRange: {
				startDate: '2024-01-16',
				endDate: '2024-01-21'
			}
		},
		{
			status: Status.WAITING_FOR_RESOURCES,
			submittedTs: new Date('2024-01-14').getTime(),
			dateRange: {
				startDate: '2024-01-14',
				endDate: '2024-01-15'
			}
		},
		{
			status: Status.RUNNING,
			submittedTs: new Date('2024-01-07').getTime(),
			dateRange: {
				startDate: '2024-01-07',
				endDate: '2024-01-13'
			}
		},
		{
			status: Status.SUCCESS,
			submittedTs: new Date('2024-01-01').getTime(),
			dateRange: {
				startDate: '2024-01-01',
				endDate: '2024-01-06'
			}
		}
	],
	mainNode: {
		name: 'job_7',
		logicalType: LogicalType.JOIN
	}
};

export const job8Response: IJobTrackerResponseArgs = {
	tasks: [
		{
			status: Status.UPSTREAM_FAILED,
			submittedTs: new Date('2024-01-19').getTime(),
			dateRange: {
				startDate: '2024-01-19',
				endDate: '2024-01-21'
			}
		},
		{
			status: Status.QUEUED,
			submittedTs: new Date('2024-01-16').getTime(),
			dateRange: {
				startDate: '2024-01-16',
				endDate: '2024-01-18'
			}
		},
		{
			status: Status.SUCCESS,
			submittedTs: new Date('2024-01-12').getTime(),
			dateRange: {
				startDate: '2024-01-12',
				endDate: '2024-01-15'
			}
		},
		{
			status: Status.UPSTREAM_FAILED,
			submittedTs: new Date('2024-01-10').getTime(),
			dateRange: {
				startDate: '2024-01-10',
				endDate: '2024-01-11'
			}
		},
		{
			status: Status.RUNNING,
			submittedTs: new Date('2024-01-05').getTime(),
			dateRange: {
				startDate: '2024-01-05',
				endDate: '2024-01-09'
			}
		},
		{
			status: Status.SUCCESS,
			submittedTs: new Date('2024-01-01').getTime(),
			dateRange: {
				startDate: '2024-01-01',
				endDate: '2024-01-04'
			}
		}
	],
	mainNode: {
		name: 'job_8',
		logicalType: LogicalType.JOIN
	}
};

export const job9Response: IJobTrackerResponseArgs = {
	tasks: [
		{
			status: Status.FAILED,
			submittedTs: new Date('2024-01-18').getTime(),
			dateRange: {
				startDate: '2024-01-18',
				endDate: '2024-01-21'
			}
		},
		{
			status: Status.QUEUED,
			submittedTs: new Date('2024-01-13').getTime(),
			dateRange: {
				startDate: '2024-01-13',
				endDate: '2024-01-17'
			}
		},
		{
			status: Status.RUNNING,
			submittedTs: new Date('2024-01-10').getTime(),
			dateRange: {
				startDate: '2024-01-10',
				endDate: '2024-01-12'
			}
		},
		{
			status: Status.FAILED,
			submittedTs: new Date('2024-01-03').getTime(),
			dateRange: {
				startDate: '2024-01-03',
				endDate: '2024-01-09'
			}
		},
		{
			status: Status.UPSTREAM_FAILED,
			submittedTs: new Date('2024-01-01').getTime(),
			dateRange: {
				startDate: '2024-01-01',
				endDate: '2024-01-02'
			}
		}
	],
	mainNode: {
		name: 'job_9',
		logicalType: LogicalType.JOIN
	}
};

export const job10Response: IJobTrackerResponseArgs = {
	tasks: [
		{
			status: Status.FAILED,
			submittedTs: new Date('2024-01-19').getTime(),
			dateRange: {
				startDate: '2024-01-19',
				endDate: '2024-01-21'
			}
		},
		{
			status: Status.QUEUED,
			submittedTs: new Date('2024-01-13').getTime(),
			dateRange: {
				startDate: '2024-01-13',
				endDate: '2024-01-18'
			}
		},
		{
			status: Status.RUNNING,
			submittedTs: new Date('2024-01-06').getTime(),
			dateRange: {
				startDate: '2024-01-06',
				endDate: '2024-01-12'
			}
		},
		{
			status: Status.UPSTREAM_FAILED,
			submittedTs: new Date('2024-01-01').getTime(),
			dateRange: {
				startDate: '2024-01-01',
				endDate: '2024-01-05'
			}
		}
	],
	mainNode: {
		name: 'job_10',
		logicalType: LogicalType.JOIN
	}
};
