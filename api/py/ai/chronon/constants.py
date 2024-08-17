try:
    from ai.chronon.scheduler.adapters.airflow_adapter import AirflowAdapter
    from ai.chronon.scheduler.adapters.temporal_adapter import TemporalAdapter
except:
    pass

ADAPTERS = {
    "airflow": AirflowAdapter,
    "temporal": TemporalAdapter
}
