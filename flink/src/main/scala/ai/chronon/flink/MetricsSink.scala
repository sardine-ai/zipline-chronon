package ai.chronon.flink
import com.codahale.metrics.ExponentiallyDecayingReservoir
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.Histogram
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction

/**
  * Sink that captures metrics around feature freshness. We capture the time taken from event creation to KV store sink
  * Ideally we expect this to match the Kafka persistence -> sink time. They can diverge if the event object is created and held on
  * in the source service for some time before the event is submitted to Kafka.
  */
class MetricsSink(groupByName: String) extends RichSinkFunction[WriteResponse] {

  @transient private var eventCreatedToSinkTimeHistogram: Histogram = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupByName)

    eventCreatedToSinkTimeHistogram = metricsGroup.histogram(
      "event_created_to_sink_time",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir())
      )
    )
  }

  override def invoke(value: WriteResponse, context: SinkFunction.Context): Unit = {
    val eventCreatedToSinkTime = System.currentTimeMillis() - value.putRequest.tsMillis.get
    eventCreatedToSinkTimeHistogram.update(eventCreatedToSinkTime)
  }
}
