package ai.chronon.service;

import ai.chronon.online.metrics.Metrics;
import ai.chronon.online.metrics.OtelMetricsReporter;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.vertx.core.Launcher;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.Label;
import io.vertx.micrometer.MicrometerMetricsFactory;
import io.vertx.micrometer.MicrometerMetricsOptions;
import java.util.Optional;

/**
 * Custom launcher to help configure the Chronon vertx feature service
 * to handle things like setting up a otel metrics registry.
 * We use otel here to be consistent with the rest of our project (e.g. fetcher code).
 * This allows us to send Vertx webservice metrics along with fetcher related metrics to allow users
 * to debug performance issues and set alerts etc.
 */
public class ChrononServiceLauncher extends Launcher {

    @Override
    public void beforeStartingVertx(VertxOptions options) {
        boolean enableMetrics = Optional.ofNullable(System.getProperty(Metrics.MetricsEnabled()))
                .map(Boolean::parseBoolean)
                .orElse(false);
        boolean isUrlDefined = OtelMetricsReporter.getExporterUrl().isDefined();

        if (enableMetrics && isUrlDefined) {
            initializeMetrics(options);
        }
    }

    private void initializeMetrics(VertxOptions options) {
        String serviceName = "ai.chronon";
        String exporterUrl = OtelMetricsReporter.getExporterUrl().get() + "/v1/metrics";
        String exportInterval = OtelMetricsReporter.MetricsExporterInterval();

        // Configure OTLP using Micrometer's built-in registry
        OtlpConfig otlpConfig = key -> {
            switch (key) {
                case "otlp.url":
                    return exporterUrl;
                case "otlp.step":
                    return exportInterval;
                case "otlp.resourceAttributes":
                    return "service.name=" + serviceName;
                default:
                    return null;
            }
        };

        MeterRegistry registry = new OtlpMeterRegistry(otlpConfig, Clock.SYSTEM);
        MicrometerMetricsFactory metricsFactory = new MicrometerMetricsFactory(registry);

        MicrometerMetricsOptions metricsOptions = new MicrometerMetricsOptions()
                .setEnabled(true)
                .setJvmMetricsEnabled(true)
                .setFactory(metricsFactory)
                .addLabels(Label.HTTP_METHOD, Label.HTTP_CODE, Label.HTTP_PATH);

        options.setMetricsOptions(metricsOptions);
    }

    public static void main(String[] args) {
        new ChrononServiceLauncher().dispatch(args);
    }
}
