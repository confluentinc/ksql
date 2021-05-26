package io.confluent.ksql.metrics;

import org.apache.kafka.common.Metric;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class CsuMetricCollector implements Runnable{

    private final String STREAM_THREAD_GROUP = "stream-thread-metrics";

    private final KafkaStreams kafkaStreams;
    private final Logger logger = LoggerFactory.getLogger(CsuMetricCollector.class);
    private final List<String> metrics;


    public CsuMetricCollector(final KafkaStreams kafkaStreams){
        this.kafkaStreams = kafkaStreams;
        this.metrics = new LinkedList<>();
        metrics.add("poll-time-total");
        metrics.add("restore-poll-time-total");
        metrics.add("send-time-total");
        metrics.add("flush-time-total");
        // just for sanity checking since this metric already exists
        metrics.add("poll-total");
    }

    @Override
    public void run() {
        logger.info("Reporting CSU metrics");
        for (String metric : metrics){
            reportMetrics(metric, STREAM_THREAD_GROUP);
        }

    }

    private void reportMetrics(final String metric, final String group) {
        final List<Metric> metricsList = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
                .filter(m -> m.metricName().name().equals(metric) &&
                        m.metricName().group().equals(group))
                .collect(Collectors.toList());
        for (Metric threadMetric : metricsList) {
            logger.info(metric + " has a value of " + threadMetric.metricValue() + " for stream thread " + threadMetric.metricName());
        }
    }
}
