package io.confluent.ksql.internal;

import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collection;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UtilizationMetricsListener implements Runnable, QueryEventListener {

    private final String STREAM_THREAD_GROUP = "stream-thread-metrics";
    private final String THREAD_ID = "thread-id";

    private final List<KafkaStreams> kafkaStreams;
    private final Logger logger = LoggerFactory.getLogger(UtilizationMetricsListener.class);
    private final List<String> metrics;
    private final Time time;
    private final long windowSize;
    private long lastSampleTime;

    private final Map<String, Double> previousPollTime;
    private final Map<String, Double> previousRestoreConsumerPollTime;
    private final Map<String, Double> previousSendTime;
    private final Map<String, Double> previousFlushTime;
    private final Map<String, Double> previousConsumerIOTime;
    private final Map<String, Double> previousProducerBufferWaitTime;

    public UtilizationMetricsListener(final long windowSize){
        this.kafkaStreams = new ArrayList<>();
        this.metrics = new LinkedList<>();
        // we can add these here or pass it in through the constructor
        metrics.add("poll-time-total");
        metrics.add("io-waittime-total");
        metrics.add("iotime-total");
        metrics.add("bufferpool-wait-time-total");
        metrics.add("restore-consumer-poll-time-total");
        metrics.add("send-time-total");
        metrics.add("flush-time-total");
        time = Time.SYSTEM;
        this.windowSize = windowSize;
        lastSampleTime = time.milliseconds();
        previousPollTime = new HashMap<>();
        previousRestoreConsumerPollTime = new HashMap<>();
        previousSendTime = new HashMap<>();
        previousFlushTime = new HashMap<>();
        previousConsumerIOTime = new HashMap<>();
        previousProducerBufferWaitTime = new HashMap<>();
    }

    // for testing
    public UtilizationMetricsListener(final long windowSize, final List<KafkaStreams> streams, final Time time, final long lastSample) {
        this.kafkaStreams = streams;
        this.metrics = new LinkedList<>();
        // we can add these here or pass it in through the constructor
        metrics.add("poll-time-total");
        metrics.add("restore-consumer-poll-time-total");
        metrics.add("send-time-total");
        metrics.add("flush-time-total");
        this.time = time;
        this.windowSize = windowSize;
        lastSampleTime = lastSample;
        previousPollTime = new HashMap<>();
        previousRestoreConsumerPollTime = new HashMap<>();
        previousSendTime = new HashMap<>();
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
        for (ThreadMetadata thread : streams.localThreadsMetadata()) {
            final String name = thread.threadName();
            previousPollTime.remove(name);
            previousRestoreConsumerPollTime.remove(name);
            previousSendTime.remove(name);
            previousFlushTime.remove(name);
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
        final BlockedTimes blockedTimes = new BlockedTimes();
        blockedTimes.internal = sampleTime - lastSampleTime;
        blockedTimes.clientLevel = sampleTime - lastSampleTime;
        for (KafkaStreams stream : kafkaStreams) {
            for (ThreadMetadata thread : stream.localThreadsMetadata()) {
                final BlockedTimes threadTimes = getProcessingRatio(thread.threadName(), stream, windowStart, windowSize);
                blockedTimes.clientLevel = Math.min(threadTimes.clientLevel, blockedTimes.clientLevel);
                blockedTimes.internal = Math.min(threadTimes.internal, blockedTimes.internal);
            }
        }
        final double notBlocked = windowSize - blockedTimes.clientLevel;
        final double notBlockedInternal = windowSize - blockedTimes.internal;
        lastSampleTime = sampleTime;
        logger.info("the current processing ratio is " + percentage(notBlocked, windowSize) + "%");
        logger.info("the current processing ratio (internal) is " + percentage(notBlockedInternal, windowSize) + "%");
        return Math.round((notBlocked / windowSize) * 100);
    }

    private double percentage(final double b, final double w) {
        return Math.round((b / w) * 100);
    }

    // public for testing
    public BlockedTimes getProcessingRatio(final String threadName, final KafkaStreams streams, final long windowStart, final double windowSize) {
        // if the type of metrics ever change in streams we'd get class cast exceptions here
        // with this behavior nothing will quit which is good, but we won't necessarily know that there's an issue
        // unless we pay attention that all the processing ratios are 100, could have a DD monitor or smthn if this
        // feels important
        try {
            final Collection<? extends Metric> allMetrics = streams.metrics().values();
            final Map<String, Double> consumerMetrics = allMetrics.stream()
                .filter(m -> m.metricName().group().equals("consumer-metrics")
                    && metrics.contains(m.metricName().name())
                    && !m.metricName().tags().getOrDefault("client-id", "").contains("restore-consumer")
                    && m.metricName().tags().getOrDefault("client-id", "").contains(threadName))
                .collect(Collectors.toMap(k -> k.metricName().name(), v -> (Double) v.metricValue()));

            final Map<String, Double> restoreConsumerMetrics = allMetrics.stream()
                .filter(m -> m.metricName().group().equals("consumer-metrics")
                    && metrics.contains(m.metricName().name())
                    && m.metricName().tags().getOrDefault("client-id", "").contains("restore-consumer")
                    && m.metricName().tags().getOrDefault("client-id", "").contains(threadName))
                .collect(Collectors.toMap(k -> k.metricName().name(), v -> (Double) v.metricValue()));

            final Map<String, Double> producerMetrics = allMetrics.stream()
                .filter(m -> m.metricName().group().equals("producer-metrics")
                    && metrics.contains(m.metricName().name())
                    && m.metricName().tags().getOrDefault("client-id", "").contains(threadName))
                .collect(Collectors.toMap(k -> k.metricName().name(), v -> (Double) v.metricValue()));

            final Map<String, Double> threadMetrics = streams.metrics().values().stream()
                    .filter(m -> m.metricName().group().equals(STREAM_THREAD_GROUP) &&
                            m.metricName().tags().get(THREAD_ID).equals(threadName) &&
                            metrics.contains(m.metricName().name()))
                    .collect(Collectors.toMap(k -> k.metricName().name(), v -> (Double) v.metricValue()));

            final List<Metric> startTimeList = streams.metrics().values().stream()
                    .filter(m -> m.metricName().group().equals(STREAM_THREAD_GROUP) &&
                            m.metricName().tags().get(THREAD_ID).equals(threadName) &&
                            m.metricName().name().equals("thread-start-time"))
                    .collect(Collectors.toList());
            final Long threadStartTime = startTimeList.size() != 0 ? (Long) startTimeList.get(0).metricValue() : 0L;

            final BlockedTimes blockedTime = new BlockedTimes();
            if (threadStartTime > windowStart) {
                blockedTime.clientLevel += threadStartTime - windowStart;
                blockedTime.internal += threadStartTime - windowStart;
            }
            final double newPollTime = threadMetrics.getOrDefault("poll-time-total", windowSize);
            final double newRestorePollTime = threadMetrics.getOrDefault("restore-consumer-poll-time-total", windowSize);
            final double newFlushTime = threadMetrics.getOrDefault("flush-time-total", windowSize);
            final double newSendTime = threadMetrics.getOrDefault("send-time-total", windowSize);
            double newConsumerIOTime = consumerMetrics.getOrDefault("io-waittime-total", windowSize)
                + consumerMetrics.getOrDefault("iotime-total", windowSize)
                + restoreConsumerMetrics.getOrDefault("io-waittime-total", windowSize)
                + restoreConsumerMetrics.getOrDefault("iotime-total", windowSize);
            newConsumerIOTime = newConsumerIOTime / (1000 * 1000);
            double newProducerBufferBlockTime = producerMetrics.getOrDefault("bufferpool-wait-time-total", windowSize);
            newProducerBufferBlockTime = newProducerBufferBlockTime / (1000 * 1000);

            blockedTime.clientLevel += Math.max(newPollTime - previousPollTime.getOrDefault(threadName, 0.0), 0);
            previousPollTime.put(threadName, newPollTime);

            blockedTime.clientLevel += Math.max(newRestorePollTime - previousRestoreConsumerPollTime.getOrDefault(threadName, 0.0), 0);
            previousRestoreConsumerPollTime.put(threadName, newRestorePollTime);

            blockedTime.clientLevel += Math.max(newSendTime - previousSendTime.getOrDefault(threadName, 0.0), 0);
            previousSendTime.put(threadName, newSendTime);

            blockedTime.clientLevel += Math.max(newFlushTime - previousFlushTime.getOrDefault(threadName, 0.0), 0);
            previousFlushTime.put(threadName, newFlushTime);

            blockedTime.internal += Math.max(newConsumerIOTime - previousConsumerIOTime.getOrDefault(threadName, 0.0), 0);
            previousConsumerIOTime.put(threadName, newConsumerIOTime);

            blockedTime.internal += Math.max(newProducerBufferBlockTime - previousProducerBufferWaitTime.getOrDefault(threadName, 0.0), 0);
            previousProducerBufferWaitTime.put(threadName, newProducerBufferBlockTime);

            blockedTime.clientLevel = Math.min(windowSize, blockedTime.clientLevel);
            blockedTime.internal = Math.min(windowSize, blockedTime.internal);
            return blockedTime;

        } catch (ClassCastException e) {
            logger.error("Class cast exception in `UtilizationMetricsListener`. The underlying" +
                    "streams metrics might have changed type." + e.getMessage());
            return new BlockedTimes();
        } catch (Exception e) {
            logger.error("oops: ", e);
            throw e;
        }
    }

    private static class BlockedTimes {
        double clientLevel = 0;
        double internal = 0;
    }
}
