/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryError.Type;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;

import java.util.Collections;
import java.util.HashMap;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.namedtopology.AddNamedTopologyResult;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SharedKafkaStreamsRuntimeImplTest {

    @Mock
    private KafkaStreamsBuilder kafkaStreamsBuilder;
    @Mock
    private KafkaStreamsNamedTopologyWrapper kafkaStreamsNamedTopologyWrapper;
    @Mock
    private KafkaStreamsNamedTopologyWrapper kafkaStreamsNamedTopologyWrapper2;
    @Mock
    private BinPackedPersistentQueryMetadataImpl binPackedPersistentQueryMetadata;
    @Mock
    private BinPackedPersistentQueryMetadataImpl binPackedPersistentQueryMetadata2;
    @Mock
    private QueryErrorClassifier queryErrorClassifier;
    @Mock
    private NamedTopology namedTopology;
    @Mock
    private AddNamedTopologyResult addNamedTopologyResult;
    @Mock
    private KafkaFuture<Void> future;

    private final QueryId queryId = new QueryId("query-1");
    private final QueryId queryId2= new QueryId("query-2");
    private Map<String, Object> streamProps = new HashMap();
    private final Map<String, String> metricsTags = Collections.singletonMap("cluster-id", "cluster-1");

    private final StreamsException query1Exception =
        new StreamsException("query down!", new TaskId(0, 0, queryId.toString()));
    private final StreamsException runtimeExceptionWithNoTask =
        new StreamsException("query down!");
    private final StreamsException runtimeExceptionWithTaskAndNoTopology =
        new StreamsException("query down!", new TaskId(0, 0));
    private final StreamsException runtimeExceptionWithTaskAndUnknownTopology =
        new StreamsException("query down!", new TaskId(0, 0, "not-a-real-query"));

    private SharedKafkaStreamsRuntimeImpl sharedKafkaStreamsRuntimeImpl;
    private MetricCollectors metricCollectors;

    @Before
    public void setUp() throws Exception {
        metricCollectors = new MetricCollectors();
        when(kafkaStreamsBuilder.buildNamedTopologyWrapper(any())).thenReturn(kafkaStreamsNamedTopologyWrapper).thenReturn(kafkaStreamsNamedTopologyWrapper2);
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "runtime");
        sharedKafkaStreamsRuntimeImpl = new SharedKafkaStreamsRuntimeImpl(
            kafkaStreamsBuilder,
            queryErrorClassifier,
            5,
            300_000L,
            streamProps,
            metricCollectors.getMetrics(),
            metricsTags
        );

        when(kafkaStreamsNamedTopologyWrapper.getTopologyByName(any())).thenReturn(Optional.empty());
        when(kafkaStreamsNamedTopologyWrapper.addNamedTopology(any())).thenReturn(addNamedTopologyResult);
        when(kafkaStreamsNamedTopologyWrapper.getAllTopologies()).thenReturn(Collections.singleton(namedTopology));
        when(namedTopology.name()).thenReturn(queryId.toString());
        when(binPackedPersistentQueryMetadata.getTopologyCopy(any())).thenReturn(namedTopology);
        when(binPackedPersistentQueryMetadata.getQueryId()).thenReturn(queryId);
        when(binPackedPersistentQueryMetadata2.getQueryId()).thenReturn(queryId2);

        sharedKafkaStreamsRuntimeImpl.register(
            binPackedPersistentQueryMetadata
        );
    }

    @Test
    public void overrideStreamsPropertiesShouldReplaceProperties() {
        // Given:
        final Map<String, Object> newProps = new HashMap<>();
        newProps.put("Test", "Test");

        // When:
        sharedKafkaStreamsRuntimeImpl.overrideStreamsProperties(newProps);

        // Then:
        final Map<String, Object> properties = sharedKafkaStreamsRuntimeImpl.streamsProperties;
        assertThat(properties.get("Test"), equalTo("Test"));
        assertThat(properties.size(), equalTo(1));
    }

    @Test
    public void shouldStartQuery() {
        //When:
        sharedKafkaStreamsRuntimeImpl.start(queryId);

        //Then:
        assertThat("Query was not added", sharedKafkaStreamsRuntimeImpl.getQueries().contains(queryId));
    }

    @Test
    public void shouldAddErrorToQuery1() {
         when(queryErrorClassifier.classify(query1Exception)).thenReturn(Type.USER);

        //Should not try to add error to query2's queue
        sharedKafkaStreamsRuntimeImpl.register(
            binPackedPersistentQueryMetadata2
        );

        //When:
        sharedKafkaStreamsRuntimeImpl.start(queryId);
        sharedKafkaStreamsRuntimeImpl.start(queryId2);

        sharedKafkaStreamsRuntimeImpl.uncaughtHandler(query1Exception);

        //Then:
        verify(binPackedPersistentQueryMetadata).setQueryError(any());
        verify(binPackedPersistentQueryMetadata2, never()).setQueryError(any());
    }

    @Test
    public void shouldAddErrorWithNoTaskToAllQueries() {
        when(queryErrorClassifier.classify(runtimeExceptionWithNoTask)).thenReturn(Type.USER);

        sharedKafkaStreamsRuntimeImpl.register(
            binPackedPersistentQueryMetadata2
        );

        //When:
        sharedKafkaStreamsRuntimeImpl.start(queryId);
        sharedKafkaStreamsRuntimeImpl.start(queryId2);

        sharedKafkaStreamsRuntimeImpl.uncaughtHandler(runtimeExceptionWithNoTask);

        //Then:
        verify(binPackedPersistentQueryMetadata).setQueryError(any());
        verify(binPackedPersistentQueryMetadata2).setQueryError(any());
    }

    @Test
    public void shouldAddErrorWithTaskAndNoTopologyToAllQueries() {
        when(queryErrorClassifier.classify(runtimeExceptionWithTaskAndNoTopology)).thenReturn(Type.USER);

        sharedKafkaStreamsRuntimeImpl.register(
            binPackedPersistentQueryMetadata2
        );

        //When:
        sharedKafkaStreamsRuntimeImpl.start(queryId);
        sharedKafkaStreamsRuntimeImpl.start(queryId2);

        sharedKafkaStreamsRuntimeImpl.uncaughtHandler(runtimeExceptionWithTaskAndNoTopology);

        //Then:
        verify(binPackedPersistentQueryMetadata).setQueryError(any());
        verify(binPackedPersistentQueryMetadata2).setQueryError(any());
    }

    @Test
    public void shouldAddErrorWithTaskAndUnknownTopologyToAllQueries() {
        when(queryErrorClassifier.classify(runtimeExceptionWithTaskAndUnknownTopology)).thenReturn(Type.USER);

        sharedKafkaStreamsRuntimeImpl.register(
            binPackedPersistentQueryMetadata2
        );

        //When:
        sharedKafkaStreamsRuntimeImpl.start(queryId);
        sharedKafkaStreamsRuntimeImpl.start(queryId2);

        sharedKafkaStreamsRuntimeImpl.uncaughtHandler(runtimeExceptionWithTaskAndUnknownTopology);

        //Then:
        verify(binPackedPersistentQueryMetadata).setQueryError(any());
        verify(binPackedPersistentQueryMetadata2).setQueryError(any());
    }
    
    @Test
    public void allLocalStorePartitionLagsCallsTopologyMethod() {
        sharedKafkaStreamsRuntimeImpl.getAllLocalStorePartitionLagsForQuery(queryId);
        verify(kafkaStreamsNamedTopologyWrapper)
            .allLocalStorePartitionLagsForTopology("query-1");
    }

    @Test
    public void shouldCloseRuntime() {
        //When:
        sharedKafkaStreamsRuntimeImpl.close();

        //Then:
        verify(kafkaStreamsNamedTopologyWrapper).close();
    }

    @Test
    public void shouldRestart() {
        //When:
        sharedKafkaStreamsRuntimeImpl.restartStreamsRuntime();

        //Then:
        verify(kafkaStreamsNamedTopologyWrapper).close();
        verify(kafkaStreamsNamedTopologyWrapper2).addNamedTopology(namedTopology);
        verify(kafkaStreamsNamedTopologyWrapper2).start();
        verify(kafkaStreamsNamedTopologyWrapper2).setUncaughtExceptionHandler((StreamsUncaughtExceptionHandler) any());
    }

    @Test
    public void shouldNotStartOrAddedToStreamsIfOnlyRegistered() {
        //Given:
        sharedKafkaStreamsRuntimeImpl.register(binPackedPersistentQueryMetadata2);

        //When:
        sharedKafkaStreamsRuntimeImpl.stop(queryId2, false);

        //Then:
        verify(binPackedPersistentQueryMetadata, never()).start();
        verify(kafkaStreamsNamedTopologyWrapper, never())
            .addNamedTopology(binPackedPersistentQueryMetadata2.getTopologyCopy(sharedKafkaStreamsRuntimeImpl));
    }

    @Test
    public void shouldRegisterMetricForQueryRestarts() {
        //Given:
        assertThat(sharedKafkaStreamsRuntimeImpl.getQueryIdSensorMap().entrySet().size(), is(1));

        //When:
        sharedKafkaStreamsRuntimeImpl.register(binPackedPersistentQueryMetadata2);

        //Then:
        assertThat(sharedKafkaStreamsRuntimeImpl.getQueryIdSensorMap().entrySet().size(), is(2));
    }

    @Test
    public void shouldRemoveSensorWhenStoppingQuery() {
        //Given:
        sharedKafkaStreamsRuntimeImpl.register(binPackedPersistentQueryMetadata2);

        //When:
        sharedKafkaStreamsRuntimeImpl.stop(queryId, false);
        sharedKafkaStreamsRuntimeImpl.stop(queryId2, true);

        //Then:
        assertThat(sharedKafkaStreamsRuntimeImpl.getQueryIdSensorMap().entrySet().size(), is(1));
        assertThat(sharedKafkaStreamsRuntimeImpl.getQueryIdSensorMap().containsKey(queryId2), is(true));
    }

    @Test
    public void shouldRecordMetricForQuery1WhenError() {
        //Given:
        when(queryErrorClassifier.classify(query1Exception)).thenReturn(Type.USER);
        sharedKafkaStreamsRuntimeImpl.register(
            binPackedPersistentQueryMetadata2
        );

        //When:
        sharedKafkaStreamsRuntimeImpl.start(queryId);
        sharedKafkaStreamsRuntimeImpl.start(queryId2);
        sharedKafkaStreamsRuntimeImpl.uncaughtHandler(query1Exception);

        //Then:
        assertThat(getMetricValue(queryId.toString(), metricsTags), is(1.0));
        assertThat(getMetricValue(queryId2.toString(), metricsTags), is(0.0));
    }

    @Test
    public void shouldRecordMetricForAllQueriesWhenErrorWithNoTask() {
        when(queryErrorClassifier.classify(runtimeExceptionWithNoTask)).thenReturn(Type.USER);

        sharedKafkaStreamsRuntimeImpl.register(
            binPackedPersistentQueryMetadata2
        );

        //When:
        sharedKafkaStreamsRuntimeImpl.start(queryId);
        sharedKafkaStreamsRuntimeImpl.start(queryId2);

        sharedKafkaStreamsRuntimeImpl.uncaughtHandler(runtimeExceptionWithNoTask);

        //Then:
        assertThat(getMetricValue(queryId.toString(), metricsTags), is(1.0));
        assertThat(getMetricValue(queryId2.toString(), metricsTags), is(1.0));
    }

    private double getMetricValue(final String queryId, final Map<String, String> metricsTags) {
        final Map<String, String> customMetricsTags = new HashMap<>(metricsTags);
        customMetricsTags.put("query_id", queryId);
        final Metrics metrics = metricCollectors.getMetrics();
        return Double.parseDouble(
            metrics.metric(
                metrics.metricName(
                    QueryMetadataImpl.QUERY_RESTART_METRIC_NAME,
                    QueryMetadataImpl.QUERY_RESTART_METRIC_GROUP_NAME,
                    QueryMetadataImpl.QUERY_RESTART_METRIC_DESCRIPTION,
                    customMetricsTags)
            ).metricValue().toString()
        );
    }
}
