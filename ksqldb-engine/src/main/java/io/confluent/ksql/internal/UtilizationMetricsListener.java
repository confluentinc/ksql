package io.confluent.ksql.internal;

import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.QueryMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.ThreadMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UtilizationMetricsListener implements Runnable, QueryEventListener {

    private final String STREAM_METRICS = "stream-thread-metrics";
    private final String CONSUMER_METRICS = "consumer-metrics";
    private final String PRODUCER_METRICS = "producer-metrics";
    private final String THREAD_ID = "thread-id";

    private final List<KafkaStreams> kafkaStreams;
    private final Logger logger = LoggerFactory.getLogger(UtilizationMetricsListener.class);
    private final List<String> metrics;
    private final Time time;
    private long lastSampleTime;

    private final Map<String, Double> previousFlushTime;
    private final Map<String, Double> previousConsumerIOTime;
    private final Map<String, Double> previousProducerBufferWaitTime;

    public UtilizationMetricsListener(){
        this.kafkaStreams = new ArrayList<>();
        this.metrics = new LinkedList<>();
        initializeMetrics();
        time = Time.SYSTEM;
        lastSampleTime = time.milliseconds();
        previousFlushTime = new HashMap<>();
        previousConsumerIOTime = new HashMap<>();
        previousProducerBufferWaitTime = new HashMap<>();
    }

    // for testing
    public UtilizationMetricsListener(final List<KafkaStreams> streams, final Time time, final long lastSample) {
        this.kafkaStreams = streams;
        this.metrics = new LinkedList<>();
        initializeMetrics();
        this.time = time;
        lastSampleTime = lastSample;
        previousFlushTime = new HashMap<>();
        previousConsumerIOTime = new HashMap<>();
        previousProducerBufferWaitTime = new HashMap<>();
    }

    @Override
    public void onCreate(
            final ServiceContext serviceContext,
            final MetaStore metaStore,
            final QueryMetadata queryMetadata) {
        kafkaStreams.add(queryMetadata.getKafkaStreams());
    }

    @Override
    public void onDeregister(final QueryMetadata query) {
        final KafkaStreams streams = query.getKafkaStreams();
        kafkaStreams.remove(streams);
        for (ThreadMetadata thread : streams.metadataForLocalThreads()) {
            final String name = thread.threadName();
            previousFlushTime.remove(name);
            previousConsumerIOTime.remove(name);
            previousProducerBufferWaitTime.remove(name);
        }
    }

    @Override
    public void run() {
        logger.info("Reporting CSU thread level metrics");
        processingRatio();

    }

    // public for testing
    public double processingRatio() {
        long sampleTime = time.milliseconds();

        final long windowSize = sampleTime - lastSampleTime;
        final long windowStart = lastSampleTime;
        double blockedTime;
        blockedTime = sampleTime - lastSampleTime;
        for (KafkaStreams stream : kafkaStreams) {
            for (ThreadMetadata thread : stream.metadataForLocalThreads()) {
                final double threadTimes = getProcessingRatio(thread.threadName(), stream, windowStart, windowSize);
                blockedTime = Math.min(threadTimes, blockedTime);
            }
        }
        final double notBlocked = windowSize - blockedTime;
        lastSampleTime = sampleTime;
        logger.info("The current processing ratio is " + percentage(notBlocked, windowSize) + "%");
        return percentage(notBlocked, windowSize);
    }

    // public for testing
    public double getProcessingRatio(final String threadName, final KafkaStreams streams, final long windowStart, final double windowSize) {
        try {
            final Collection<? extends Metric> allMetrics = streams.metrics().values();
            final Map<String, Double> consumerMetrics = allMetrics.stream()
                .filter(m -> m.metricName().group().equals(CONSUMER_METRICS)
                    && metrics.contains(m.metricName().name())
                    && !m.metricName().tags().getOrDefault("client-id", "").contains("global-consumer")
                    && !m.metricName().tags().getOrDefault("client-id", "").contains("restore-consumer")
                    && m.metricName().tags().getOrDefault("client-id", "").contains(threadName))
                .collect(Collectors.toMap(k -> k.metricName().name(), v -> (Double) v.metricValue()));

            final Map<String, Double> restoreConsumerMetrics = allMetrics.stream()
                .filter(m -> m.metricName().group().equals(CONSUMER_METRICS)
                    && metrics.contains(m.metricName().name())
                    && !m.metricName().tags().getOrDefault("client-id", "").contains("global-consumer")
                    && m.metricName().tags().getOrDefault("client-id", "").contains("restore-consumer")
                    && m.metricName().tags().getOrDefault("client-id", "").contains(threadName))
                .collect(Collectors.toMap(k -> k.metricName().name(), v -> (Double) v.metricValue()));

            final Map<String, Double> producerMetrics = allMetrics.stream()
                .filter(m -> m.metricName().group().equals(PRODUCER_METRICS)
                    && metrics.contains(m.metricName().name())
                    && m.metricName().tags().getOrDefault("client-id", "").contains(threadName))
                .collect(Collectors.toMap(k -> k.metricName().name(), v -> (Double) v.metricValue()));

            final Map<String, Double> threadMetrics = allMetrics.stream()
                    .filter(m -> m.metricName().group().equals(STREAM_METRICS) &&
                            m.metricName().tags().getOrDefault(THREAD_ID, "").equals(threadName) &&
                            metrics.contains(m.metricName().name()))
                    .collect(Collectors.toMap(k -> k.metricName().name(), v -> (Double) v.metricValue()));

            final List<Metric> startTimeList = allMetrics.stream()
                    .filter(m -> m.metricName().name().equals("thread-start-time") &&
                            m.metricName().tags().getOrDefault(THREAD_ID, "").equals(threadName))
                    .collect(Collectors.toList());
            final Long threadStartTime = startTimeList.size() != 0 ? (Long) startTimeList.get(0).metricValue() : 0L;

            double blockedTime = 0.0;
            if (threadStartTime > windowStart) {
                blockedTime += threadStartTime - windowStart;
            }
            final double newFlushTime = threadMetrics.getOrDefault("flush-time-total", 0.0);
            double newConsumerIOTime = consumerMetrics.getOrDefault("io-waittime-total", 0.0)
                + consumerMetrics.getOrDefault("iotime-total", 0.0)
                + restoreConsumerMetrics.getOrDefault("io-waittime-total", 0.0)
                + restoreConsumerMetrics.getOrDefault("iotime-total", 0.0);
            newConsumerIOTime = newConsumerIOTime / (1000 * 1000);

            double newProducerBufferBlockTime = producerMetrics.getOrDefault("bufferpool-wait-time-total", 0.0);
            newProducerBufferBlockTime = newProducerBufferBlockTime / (1000 * 1000);

            blockedTime += Math.max(newFlushTime - previousFlushTime.getOrDefault(threadName, 0.0), 0);
            previousFlushTime.put(threadName, newFlushTime);

            blockedTime += Math.max(newConsumerIOTime - previousConsumerIOTime.getOrDefault(threadName, 0.0), 0);
            previousConsumerIOTime.put(threadName, newConsumerIOTime);

            blockedTime += Math.max(newProducerBufferBlockTime - previousProducerBufferWaitTime.getOrDefault(threadName, 0.0), 0);
            previousProducerBufferWaitTime.put(threadName, newProducerBufferBlockTime);

            blockedTime = Math.min(windowSize, blockedTime);
            return blockedTime;

        } catch (ClassCastException e) {
            logger.error("Class cast exception in `UtilizationMetricsListener`. The underlying" +
                    "streams metrics might have changed type." + e.getMessage());
            return 0.0;
        } catch (Exception e) {
            logger.error("oops: ", e);
            throw e;
        }
    }

    private void initializeMetrics() {
        metrics.add("io-waittime-total");
        metrics.add("iotime-total");
        metrics.add("bufferpool-wait-time-total");
        metrics.add("flush-time-total");
    }

    private double percentage(final double b, final double w) {
        return Math.round((b / w) * 100);
    }
}
