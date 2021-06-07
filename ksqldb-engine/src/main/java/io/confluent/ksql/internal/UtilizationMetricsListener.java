package io.confluent.ksql.internal;

import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.QueryMetadata;
import org.apache.kafka.common.Metric;
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

    private final List<KafkaStreams> kafkaStreams;
    private final Logger logger = LoggerFactory.getLogger(UtilizationMetricsListener.class);
    private final List<String> metrics;
    private final Time time;

    private final Map<String, Double> previousPollTime;
    private final Map<String, Double> previousRestoreConsumerPollTime;
    private final Map<String, Double> previousSendTime;
    private final Map<String, Double> previousFlushTime;

    public UtilizationMetricsListener(){
        this.kafkaStreams = new ArrayList<>();
        this.metrics = new LinkedList<>();
        // we can add these here or pass it in through the constructor
        metrics.add("poll-time-total");
        metrics.add("restore-consumer-poll-time-total");
        metrics.add("send-time-total");
        metrics.add("flush-time-total");
// just for sanity checking since this metric already exists
        metrics.add("poll-total");
        time = Time.SYSTEM;
        previousPollTime = new HashMap<>();
        previousRestoreConsumerPollTime = new HashMap<>();
        previousSendTime = new HashMap<>();
        previousFlushTime = new HashMap<>();
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
        // Question - if we terminate a query and then restart it, will the underling
        // kafka streams have a new name? if so, do we want to remove everything from the
        // previous value hashmaps? Or do we want to see that historical information still?
        // Seems like if there's a chance we could re-use the name we'd want a clean slate
        kafkaStreams.remove(query.getKafkaStreams());
    }

    @Override
    public void run() {
        logger.info("Reporting CSU system level metrics");
        reportSystemMetrics();
        logger.info("Reporting CSU thread level metrics");
        reportProcessingRatio();
        /*for (KafkaStreams thread : kafkaStreams) {
            for (String metric : metrics) {
                reportThreadMetrics(thread, metric, STREAM_THREAD_GROUP);
            }
        }*/
    }

    private void reportSystemMetrics() {
        try {
            BufferedReader br = new BufferedReader(new FileReader("/procfs/sys/1/stat"));
            String[] columns = br.readLine().split(" ");
            String timeUser = columns[13];
            String timeKernel = columns[14];
            int clockTicks = 100;

            logger.info("we've got " + timeUser + " for timeuser, " + timeKernel + " for timekernel, not sure about clock ticks");
        } catch(Exception e) {
            logger.info("something went wrong finding cpu utilization metrics " + e.getMessage());
        }

        logger.info("we're using some disk");
    }

    private void reportProcessingRatio() {
        final double totalTime = 120000.0;
        double blockedTime = 120000.0;

        final long windowEnd = time.milliseconds();
        final long windowStart = (long) Math.max(0, windowEnd - totalTime);
        logger.info("--- window start: " + windowStart + " ----");
        for (KafkaStreams stream : kafkaStreams) {
            for (ThreadMetadata thread : stream.localThreadsMetadata()) {
                blockedTime = Math.min(getProcessingRatio(thread.threadName(), stream, windowStart, totalTime), totalTime);
            }
        }
        final double notBlocked = totalTime - blockedTime;
        final double processingRatio = (notBlocked / totalTime) * 100;
        logger.info("total time - blocked time = " + (totalTime - blockedTime));
        logger.info("the current processing ratio is " + processingRatio + "%");
    }

    private double getProcessingRatio(final String threadName, final KafkaStreams streams, final long windowStart, final double windowSize) {
        final Map<String, Double> threadMetrics = streams.metrics().values().stream()
                .filter(m -> m.metricName().group().equals("stream-thread-metrics") &&
                        m.metricName().tags().get("thread-id").equals(threadName) &&
                        metrics.contains(m.metricName().name()))
                .collect(Collectors.toMap(k -> k.metricName().name(), v -> (double) v.metricValue()));
        final Long threadStartTime = (Long) streams.metrics().values().stream()
                .filter(m -> m.metricName().group().equals("stream-thread-metrics") &&
                        m.metricName().tags().get("thread-id").equals(threadName) &&
                        m.metricName().name().equals("thread-start-time")).collect(Collectors.toList()).get(0).metricValue();
        logger.info("--- thread start: " + threadStartTime + " ----");
        double blockedTime = 0;
        if (threadStartTime > windowStart || threadStartTime == 0.0) {
            logger.info("in fact, the thread was started after the window");
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
        logger.info("added " + (newPollTime - previousPollTime.get(threadName)) + " to blocked time");
        previousPollTime.put(threadName, newPollTime);

        blockedTime += Math.max(newRestorePollTime - previousRestoreConsumerPollTime.get(threadName), 0);
        previousRestoreConsumerPollTime.put(threadName, newRestorePollTime);

        blockedTime += Math.max(newSendTime - previousSendTime.get(threadName), 0);
        previousSendTime.put(threadName, newSendTime);
        blockedTime += Math.max(newFlushTime - previousFlushTime.get(threadName), 0);
        previousFlushTime.put(threadName, newFlushTime);

        return Math.min(windowSize, blockedTime);
    }

    private void reportThreadMetrics(final KafkaStreams thread, final String metric, final String group) {
        final List<Metric> metricsList = new ArrayList<Metric>(thread.metrics().values()).stream()
                .filter(m -> m.metricName().name().equals(metric) &&
                        m.metricName().group().equals(group))
                .collect(Collectors.toList());
        for (Metric threadMetric : metricsList) {
            logger.info(metric + " has a value of " + threadMetric.metricValue() + " for stream thread " + thread);
        }
    }
}
