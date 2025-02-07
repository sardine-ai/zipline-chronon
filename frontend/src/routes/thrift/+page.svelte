<!-- todo: this is only for testing, will be deleted before merge -->
<script lang="ts">
	import { Api } from '$lib/api/api';

	$effect(() => {
		const api = new Api({ fetch });

		api
			.getJoin('risk.user_transactions.txn_join')
			.then((join) => {
				console.log('Join data:', join);
			})
			.catch((err) => {
				console.error('Error fetching join:', err);
			});

		api
			.getGroupBy('risk.transaction_events.txn_group_by_user')
			.then((groupBy) => {
				console.log('GroupBy data:', groupBy);
			})
			.catch((err) => {
				console.error('Error fetching groupBy:', err);
			});

		api
			.getModel('quickstart.test.v1')
			.then((model) => {
				console.log('Model data:', model);
			})
			.catch((err) => {
				console.error('Error fetching model:', err);
			});

		api
			.getStagingQuery('todo') // doesnt work, no staging queries defined
			.then((query) => {
				console.log('Staging Query data:', query);
			})
			.catch((err) => {
				console.error('Error fetching staging query:', err);
			});

		api
			.getJoinList()
			.then((joins) => {
				console.log('Join list:', joins);
			})
			.catch((err) => {
				console.error('Error fetching join list:', err);
			});

		api
			.getGroupByList()
			.then((groupBys) => {
				console.log('GroupBy list:', groupBys);
			})
			.catch((err) => {
				console.error('Error fetching groupBy list:', err);
			});

		api
			.getModelList()
			.then((models) => {
				console.log('Model list:', models);
			})
			.catch((err) => {
				console.error('Error fetching model list:', err);
			});

		api
			.getStagingQueryList()
			.then((queries) => {
				console.log('Staging query list:', queries);
			})
			.catch((err) => {
				console.error('Error fetching staging query list:', err);
			});

		api
			.getJoinTimeseries({
				joinId: 'risk.user_transactions.txn_join',
				startTs: 1673308800000,
				endTs: 1674172800000
			})
			.then((oldData) => {
				console.log('Old drift data:', oldData);
			});

		api
			.getJoinDrift({
				name: 'risk.user_transactions.txn_join',
				startTs: 1673308800000,
				endTs: 1674172800000
			})
			.then((newData) => {
				console.log('New drift data:', newData);
			});

		api
			.getFeatureTimeseries({
				joinId: 'risk.user_transactions.txn_join',
				featureName: 'dim_merchant_preferred_language',
				startTs: 1673308800000,
				endTs: 1674172800000
			})
			.then((oldData) => {
				console.log('Old feature drift data:', oldData);
			});

		api
			.getColumnDrift({
				name: 'risk.user_transactions.txn_join',
				columnName: 'dim_merchant_preferred_language',
				startTs: 1673308800000,
				endTs: 1674172800000
			})
			.then((newData) => {
				console.log('New column drift data:', newData);
			});

		api
			.getColumnSummary({
				name: 'risk.user_transactions.txn_join',
				columnName: 'dim_merchant_preferred_language',
				startTs: 1673308800000,
				endTs: 1674172800000
			})
			.then((columnSummary) => {
				console.log('Column summary data:', columnSummary);
			});

		api
			.getColumnSummary({
				name: 'risk.user_transactions.txn_join',
				columnName: 'dim_merchant_account_type',
				startTs: 1673308800000,
				endTs: 1674172800000
			})
			.then((columnSummary) => {
				console.log('Merchant account type summary:', columnSummary);
			});
	});
</script>

<div>hello!</div>
