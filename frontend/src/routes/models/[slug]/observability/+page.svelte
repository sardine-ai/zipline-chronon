<script lang="ts">
	const { data } = $props();
	const timeseries = $state(data.timeseries);
</script>

<div>Observability for Model {timeseries.id}</div>
<div>Timeseries below (to be replaced with a line chart)</div>

<pre>{JSON.stringify(timeseries, null, 2)}</pre>
