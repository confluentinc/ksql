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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryId;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.namedtopology.AddNamedTopologyResult;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.checkerframework.checker.units.qual.A;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@RunWith(MockitoJUnitRunner.class)
public class SandboxedSharedKafkaStreamsRuntimeImplTest {

  @Mock
  private KafkaStreamsBuilder kafkaStreamsBuilder;
  @Mock
  private KafkaStreamsNamedTopologyWrapper kafkaStreamsNamedTopologyWrapper;
  @Mock
  private KafkaStreamsNamedTopologyWrapper kafkaStreamsNamedTopologyWrapper2;
  @Mock
  private BinPackedPersistentQueryMetadataImpl binPackedPersistentQueryMetadata;
  @Mock
  private NamedTopology topology;
  @Mock
  private QueryId queryId;
  @Mock
  private QueryId queryId2;
  @Mock
  private AddNamedTopologyResult addNamedTopologyResult;
  @Mock
  private KafkaFuture<Void> future;

  private SandboxedSharedKafkaStreamsRuntimeImpl validationSharedKafkaStreamsRuntime;

  @Before
  public void setUp() throws Exception {
    final Map<String, Object> streamProps = new HashMap<>();
    streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "SharedRuntimeId-validation");
    when(kafkaStreamsNamedTopologyWrapper.addNamedTopology(any())).thenReturn(addNamedTopologyResult);
    when(addNamedTopologyResult.all()).thenReturn(future);
    when(kafkaStreamsNamedTopologyWrapper.getTopologyByName(any())).thenReturn(Optional.empty());
    when(kafkaStreamsBuilder.buildNamedTopologyWrapper(any())).thenReturn(kafkaStreamsNamedTopologyWrapper).thenReturn(kafkaStreamsNamedTopologyWrapper2);
    validationSharedKafkaStreamsRuntime = new SandboxedSharedKafkaStreamsRuntimeImpl(
        kafkaStreamsBuilder,
        streamProps
    );
    when(queryId.toString()).thenReturn("query 1");

    when(binPackedPersistentQueryMetadata.getTopologyCopy(any())).thenReturn(topology);
    when(binPackedPersistentQueryMetadata.getQueryId()).thenReturn(queryId);

    validationSharedKafkaStreamsRuntime.register(
        binPackedPersistentQueryMetadata
    );
  }

  @Test
  public void shouldStartQuery() {
    //When:
    validationSharedKafkaStreamsRuntime.start(queryId);

    //Then:
    assertThat("Query was not added", validationSharedKafkaStreamsRuntime.getQueries().contains(queryId));
  }

  @Test
  public void shouldNotStopQuery() {
    //Given:
    validationSharedKafkaStreamsRuntime.start(queryId);

    //When:
    validationSharedKafkaStreamsRuntime.stop(queryId, false);

    //Then:
    assertThat("Query was stopped", validationSharedKafkaStreamsRuntime.getQueries().contains(queryId));
  }

  @Test
  public void shouldCloseRuntime() {
    //When:
    validationSharedKafkaStreamsRuntime.close();

    //Then:
    verify(kafkaStreamsNamedTopologyWrapper).close();
  }

  @Test
  public void shouldAddTopologyDuringRegister() {

    //Then:
    verify(kafkaStreamsNamedTopologyWrapper).getTopologyByName(queryId.toString());
    verify(kafkaStreamsNamedTopologyWrapper).addNamedTopology(topology);
  }
}