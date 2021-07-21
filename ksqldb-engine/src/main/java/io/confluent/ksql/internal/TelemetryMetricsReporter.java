package io.confluent.ksql.internal;

import com.google.common.collect.ImmutableMap;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.MetricsUtils;
import io.confluent.telemetry.metrics.SinglePointMetric;
import io.confluent.telemetry.provider.KsqlProvider;
import io.confluent.telemetry.provider.ProviderRegistry;
import io.confluent.telemetry.reporter.TelemetryReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.utils.AppInfoParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TelemetryMetricsReporter implements MetricsReporter {

    public static final String RESOURCE_LABEL_PREFIX = "resource.";
    public static final String RESOURCE_LABEL_TYPE = RESOURCE_LABEL_PREFIX + "type";
    public static final String RESOURCE_LABEL_VERSION = RESOURCE_LABEL_PREFIX + "version";
    public static final String RESOURCE_LABEL_COMMIT_ID = RESOURCE_LABEL_PREFIX + "commit.id";
    public static final String RESOURCE_CLUSTER_ID = RESOURCE_LABEL_PREFIX + "cluster.id";

    private final Logger logger = LoggerFactory.getLogger(TelemetryMetricsReporter.class);
    private final TelemetryReporter reporter;
    private final Map<String, String> metricTags = new HashMap<>();
    // ToDo: How is the cluster ID passed to this class
    private final String clusterId = "";
    private final String metricGroup;


    public TelemetryMetricsReporter(
            final String metricGroup,
            final String providerNamespace,
            final String resourceType,
            final Map<String, ?> config
    ) {
        this(metricGroup, providerNamespace, resourceType, config, new TelemetryReporter());
    }

    protected TelemetryMetricsReporter(
            final String metricGroup,
            final String providerNamespace,
            final String resourceType,
            final Map<String, ?> config,
            final TelemetryReporter reporter
    ) {
        this.metricGroup = metricGroup;
        this.reporter = reporter;
        configureTelemetryReporter(providerNamespace, resourceType, config, reporter);
    }

    @Override
    public void report(final List<DataPoint> dataPoints) {
        for (final DataPoint dataPoint : dataPoints) {
            emitMetric(
                    dataPoint.getName(),
                    dataPoint.getTime(),
                    dataPoint.getValue(),
                    ImmutableMap.<String, String>builder().putAll(metricTags).putAll(dataPoint.getTags()).build()
            );
        }
    }

    void emitMetric(
            final String metricName,
            final Instant timestamp,
            final Object value,
            final Map<String, String> metricTags
    ) {
        // Todo: Is it correct to use the KsqlProvider provided by ce-kafka?
        final String fullMetricName = MetricsUtils.fullMetricName(
                KsqlProvider.DOMAIN, metricGroup, metricName);
        final MetricKey key = new MetricKey(fullMetricName, metricTags);

        reporter.emitter().emitMetric(SinglePointMetric.gauge(key, (Double) value, timestamp));
    }

    private void configureTelemetryReporter(
            final String providerNamespace,
            final String resourceType,
            final Map<String, ?> config,
            final TelemetryReporter reporter
    ) {
        // Todo: Is it correct to use the KsqlProvider provided by ce-kafka?
        ProviderRegistry.registerProvider(
                providerNamespace,
                KsqlProvider.class.getCanonicalName()
        );

        reporter.configure(config);
        MetricsContext ctx = createMetricContext(providerNamespace, resourceType);
        try {
            reporter.contextChange(ctx);
        } catch (final Throwable e) {
            logger.error("Failed to properly start up telemetry reporter with the following error: ", e);
            throw e;
        }
    }

    private MetricsContext createMetricContext(
            final String providerNameSpace,
            final String resourcetype
    ) {
        Map<String, Object> rawConfig = new HashMap<>();
        rawConfig.put(RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
        rawConfig.put(RESOURCE_LABEL_TYPE, resourcetype);
        rawConfig.put(RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
        rawConfig.put(RESOURCE_CLUSTER_ID, clusterId);

        return new KafkaMetricsContext(providerNameSpace, rawConfig);
    }

    @Override
    public void close() {
        reporter.close();
    }
}
