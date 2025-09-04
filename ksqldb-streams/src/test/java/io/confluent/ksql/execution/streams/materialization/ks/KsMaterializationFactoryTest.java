/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams.materialization.ks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializationFactory.LocatorFactory;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializationFactory.MaterializationFactory;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializationFactory.StateStoreFactory;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsMaterializationFactoryTest {

  private static final String APPLICATION_ID = "app_id";
  private static final String STORE_NAME = "someStore";
  private static final URL DEFAULT_APP_SERVER = buildDefaultAppServer();

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.DOUBLE)
      .build();

  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private Topology topology;
  @Mock
  private Serializer<GenericKey> keySerializer;

  @Mock
  private LocatorFactory locatorFactory;
  @Mock
  private KsLocator locator;
  @Mock
  private StateStoreFactory storeFactory;
  @Mock
  private KsStateStore stateStore;
  @Mock
  private MaterializationFactory materializationFactory;
  @Mock
  private KsMaterialization materialization;
  @Mock
  private KsqlConfig ksqlConfig;
  private KsMaterializationFactory factory;
  private final Map<String, Object> streamsProperties = new HashMap<>();

  @Before
  public void setUp() {
    factory = new KsMaterializationFactory(
        locatorFactory,
        storeFactory,
        materializationFactory
    );

    when(locatorFactory.create(any(), any(), any(), any(), any(),
        anyBoolean(), anyString())).thenReturn(locator);
    when(storeFactory.create(any(), any(), any(), any(), any())).thenReturn(stateStore);
    when(materializationFactory.create(any(), any(), any())).thenReturn(materialization);

    streamsProperties.clear();
    streamsProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, DEFAULT_APP_SERVER.toString());
  }

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(LocatorFactory.class, locatorFactory)
        .setDefault(StateStoreFactory.class, storeFactory)
        .setDefault(MaterializationFactory.class, materializationFactory)
        .testConstructors(KsMaterializationFactory.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldReturnEmptyIfAppServerNotConfigured() {
    // Given:
    streamsProperties.remove(StreamsConfig.APPLICATION_SERVER_CONFIG);

    // When:
    final Optional<KsMaterialization> result = factory
        .create(STORE_NAME, kafkaStreams, topology, SCHEMA, keySerializer, Optional.empty(),
            streamsProperties, ksqlConfig, APPLICATION_ID, "queryId");

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldBuildLocatorWithCorrectParams() {
    // When:
    factory.create(STORE_NAME, kafkaStreams, topology, SCHEMA, keySerializer, Optional.empty(),
        streamsProperties, ksqlConfig, APPLICATION_ID, "queryId");

    // Then:
    verify(locatorFactory).create(
        STORE_NAME,
        kafkaStreams,
        topology,
        keySerializer,
        DEFAULT_APP_SERVER,
        false,
        "queryId"
    );
  }

  @Test
  public void shouldBuildStateStoreWithCorrectParams() {
    // When:
    factory.create(STORE_NAME, kafkaStreams, topology, SCHEMA, keySerializer, Optional.empty(),
        streamsProperties, ksqlConfig, APPLICATION_ID, "queryId");

    // Then:
    verify(storeFactory).create(
        STORE_NAME,
        kafkaStreams,
        SCHEMA,
        ksqlConfig,
        "queryId"
    );
  }

  @Test
  public void shouldBuildMaterializationWithCorrectParams() {
    // Given:
    final Optional<WindowInfo> windowInfo =
        Optional.of(WindowInfo.of(WindowType.SESSION, Optional.empty(), Optional.empty()));

    // When:
    factory.create(STORE_NAME, kafkaStreams, topology, SCHEMA, keySerializer, windowInfo,
        streamsProperties, ksqlConfig, APPLICATION_ID, anyString());

    // Then:
    verify(materializationFactory).create(
        windowInfo,
        locator,
        stateStore
    );
  }

  @Test
  public void shouldReturnMaterialization() {
    // When:
    final Optional<KsMaterialization> result = factory
        .create(STORE_NAME, kafkaStreams, topology, SCHEMA, keySerializer, Optional.empty(),
            streamsProperties, ksqlConfig, APPLICATION_ID, any());

    // Then:
    assertThat(result,  is(Optional.of(materialization)));
  }

  private static URL buildDefaultAppServer() {
    try {
      return new URL("https://someHost:9876");
    } catch (final MalformedURLException e) {
      throw new AssertionError("Failed to build app server URL");
    }
  }
}