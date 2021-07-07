package io.confluent.ksql.internal;

import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.QueryMetadata;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class UtilizationMetricsListener implements Runnable, QueryEventListener {

    private final List<KafkaStreams> kafkaStreams;
    private final Logger logger = LoggerFactory.getLogger(UtilizationMetricsListener.class);
    private final List<String> metrics;
    private final Time time;
    private long lastSampleTime;

    public UtilizationMetricsListener(){
        this.kafkaStreams = new ArrayList<>();
        this.metrics = new LinkedList<>();
        time = Time.SYSTEM;
        lastSampleTime = time.milliseconds();
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
    }

    @Override
    public void run() {
        logger.info("Reporting Observability Metrics");
        final Long currentTime = time.milliseconds();
        // here is where we would report metrics
        lastSampleTime = currentTime;
    }
}
