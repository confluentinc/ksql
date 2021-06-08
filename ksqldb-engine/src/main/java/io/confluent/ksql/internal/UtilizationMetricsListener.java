package io.confluent.ksql.internal;

import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.QueryMetadata;
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
    public Map<String, Double> temporaryThreadMetrics;

    public UtilizationMetricsListener(final long windowSize){
        this.kafkaStreams = new ArrayList<>();
        this.metrics = new LinkedList<>();
        // we can add these here or pass it in through the constructor
        metrics.add("poll-time-total");
        metrics.add("restore-consumer-poll-time-total");
        metrics.add("send-time-total");
        metrics.add("flush-time-total");
        time = Time.SYSTEM;
        this.windowSize = windowSize;
        lastSampleTime = 0L;
        previousPollTime = new HashMap<>();
        previousRestoreConsumerPollTime = new HashMap<>();
        previousSendTime = new HashMap<>();
        previousFlushTime = new HashMap<>();
        temporaryThreadMetrics = new HashMap<>();
    }

    // for testing
    public UtilizationMetricsListener(final long windowSize, final List<KafkaStreams> streams) {
        this.kafkaStreams = streams;
        this.metrics = new LinkedList<>();
        // we can add these here or pass it in through the constructor
        metrics.add("poll-time-total");
        metrics.add("restore-consumer-poll-time-total");
        metrics.add("send-time-total");
        metrics.add("flush-time-total");
        time = Time.SYSTEM;
        this.windowSize = windowSize;
        previousPollTime = new HashMap<>();
        previousRestoreConsumerPollTime = new HashMap<>();
        previousSendTime = new HashMap<>();
        previousFlushTime = new HashMap<>();
        temporaryThreadMetrics = new HashMap<>();
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
        kafkaStreams.remove(query.getKafkaStreams());
        previousPollTime.remove("poll-time-total");
        previousRestoreConsumerPollTime.remove("restore-consumer-poll-time-total");
        previousSendTime.remove("send-poll-time-total");
        previousFlushTime.remove("flush-poll-time-total");
    }

    @Override
    public void run() {
        logger.info("Reporting CSU thread level metrics");
        while (true) {
            logger.info("the current processing ratio is " + processingRatio() + "%");
            try {
                Thread.sleep(windowSize);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void reportSystemMetrics() {
        try {
            BufferedReader br = new BufferedReader(new FileReader("/procfs/sys/1/stat"));
            String[] columns = br.readLine().split(" ");
            String timeUser = columns[13];
            String timeKernel = columns[14];
            int clockTicks = 100;

            logger.info("we've got " + timeUser + " for timeuser, " + timeKernel + " for timekernel");
        } catch(Exception e) {
            logger.info("something went wrong finding cpu utilization metrics " + e.getMessage());
        }

        logger.info("we're using some disk");
    }

    // public for testing
    public double processingRatio() {
        long sampleTime = time.milliseconds();
        double blockedTime = sampleTime - lastSampleTime;

        final long windowSize = sampleTime - lastSampleTime;
        final long windowStart = lastSampleTime;
        for (KafkaStreams stream : kafkaStreams) {
            for (ThreadMetadata thread : stream.localThreadsMetadata()) {
                blockedTime = Math.min(getProcessingRatio(thread.threadName(), stream, windowStart, windowSize), blockedTime);
            }
        }
        final double notBlocked = windowSize - blockedTime;
        lastSampleTime = sampleTime;
        return (notBlocked / windowSize) * 100;
    }

    // public for testing
    public double getProcessingRatio(final String threadName, final KafkaStreams streams, final long windowStart, final double windowSize) {
        final Map<String, Double> threadMetrics = streams.metrics().values().stream()
                .filter(m -> m.metricName().group().equals(STREAM_THREAD_GROUP) &&
                        m.metricName().tags().get(THREAD_ID).equals(threadName) &&
                        metrics.contains(m.metricName().name()))
                .collect(Collectors.toMap(k -> k.metricName().name(), v -> (double) v.metricValue()));
        // for testing
        temporaryThreadMetrics = threadMetrics;
        final Long threadStartTime = (Long) streams.metrics().values().stream()
                .filter(m -> m.metricName().group().equals(STREAM_THREAD_GROUP) &&
                        m.metricName().tags().get(THREAD_ID).equals(threadName) &&
                        m.metricName().name().equals("thread-start-time"))
                .collect(Collectors.toList())
                .get(0)
                .metricValue();
        double blockedTime = 0;
        if (threadStartTime > windowStart) {
            blockedTime += threadStartTime - windowStart;
            previousPollTime.put(threadName, 0.0);
            previousRestoreConsumerPollTime.put(threadName, 0.0);
            previousSendTime.put(threadName, 0.0);
            previousFlushTime.put(threadName, 0.0);
        }
        final double newPollTime = threadMetrics.getOrDefault("poll-time-total", 0.0);
        final double newRestorePollTime = threadMetrics.getOrDefault("restore-consumer-poll-time-total", 0.0);
        final double newFlushTime = threadMetrics.getOrDefault("flush-time-total", 0.0);
        final double newSendTime = threadMetrics.getOrDefault("send-time-total", 0.0);
        blockedTime += Math.max(newPollTime - previousPollTime.get(threadName), 0);
        previousPollTime.put(threadName, newPollTime);

        blockedTime += Math.max(newRestorePollTime - previousRestoreConsumerPollTime.get(threadName), 0);
        previousRestoreConsumerPollTime.put(threadName, newRestorePollTime);

        blockedTime += Math.max(newSendTime - previousSendTime.get(threadName), 0);
        previousSendTime.put(threadName, newSendTime);

        blockedTime += Math.max(newFlushTime - previousFlushTime.get(threadName), 0);
        previousFlushTime.put(threadName, newFlushTime);

        return Math.min(windowSize, blockedTime);
    }
}
