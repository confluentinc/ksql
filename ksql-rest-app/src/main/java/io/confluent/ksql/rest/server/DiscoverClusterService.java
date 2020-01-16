package io.confluent.ksql.rest.server;

import static java.util.Objects.requireNonNull;

import com.google.common.util.concurrent.AbstractScheduledService;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;

