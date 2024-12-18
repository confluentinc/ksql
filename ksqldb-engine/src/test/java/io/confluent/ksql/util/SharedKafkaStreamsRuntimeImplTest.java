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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryError.Type;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.namedtopology.AddNamedTopologyResult;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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

    private final QueryId queryId = new QueryId("query-1");
    private final QueryId queryId2= new QueryId("query-2");
    private final Map<String, Object> streamProps = new HashMap<>();

    private final StreamsException query1Exception =
        new StreamsException("query down!", new TaskId(0, 0, queryId.toString()));
    private final StreamsException runtimeExceptionWithNoTask =
        new StreamsException("query down!");
    private final StreamsException runtimeExceptionWithTaskAndNoTopology =
        new StreamsException("query down!", new TaskId(0, 0));
    private final StreamsException runtimeExceptionWithTaskAndUnknownTopology =
        new StreamsException("query down!", new TaskId(0, 0, "not-a-real-query"));

    private SharedKafkaStreamsRuntimeImpl sharedKafkaStreamsRuntimeImpl;

    @Before
    public void setUp() throws Exception {
        when(kafkaStreamsBuilder.buildNamedTopologyWrapper(any())).thenReturn(kafkaStreamsNamedTopologyWrapper).thenReturn(kafkaStreamsNamedTopologyWrapper2);
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "runtime");
        streamProps.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "old");
        sharedKafkaStreamsRuntimeImpl = new SharedKafkaStreamsRuntimeImpl(
            kafkaStreamsBuilder,
            queryErrorClassifier,
            5,
            300_000L,
            streamProps
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
        newProps.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "notused");

        // When:
        sharedKafkaStreamsRuntimeImpl.overrideStreamsProperties(newProps);

        // Then:
        final Map<String, Object> properties = sharedKafkaStreamsRuntimeImpl.streamsProperties;
        assertThat(properties.get(StreamsConfig.APPLICATION_SERVER_CONFIG), equalTo("old"));
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
        when(binPackedPersistentQueryMetadata.getQueryStatus()).thenReturn(KsqlConstants.KsqlQueryStatus.PAUSED);
        sharedKafkaStreamsRuntimeImpl.restartStreamsRuntime();

        //Then:
        verify(kafkaStreamsNamedTopologyWrapper).close();
        verify(kafkaStreamsNamedTopologyWrapper2).pauseNamedTopology(any());
        verify(kafkaStreamsNamedTopologyWrapper2).addNamedTopology(any());
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
}
