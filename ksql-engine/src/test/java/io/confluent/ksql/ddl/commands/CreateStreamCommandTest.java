/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.ddl.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CreateStreamCommandTest {

  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private CreateStream createStreamStatement;

  @Before
  public void setUp() {
    givenPropertiesWith((Collections.emptyMap()));
    when(createStreamStatement.getName()).thenReturn(QualifiedName.of("name"));
    when(createStreamStatement.getElements()).thenReturn(ImmutableList.of());
    when(topicClient.isTopicExists(any())).thenReturn(true);
  }

  @Test
  public void shouldDefaultToStringKeySerde() {
    // When:
    final CreateStreamCommand cmd = createCmd();

    // Then:
    assertThat(cmd.keySerde, is(instanceOf(Serdes.String().getClass())));
  }

  @Test
  public void shouldExtractSessionWindowType() {
    // Given:
    givenPropertiesWith(ImmutableMap.of(
        DdlConfig.WINDOW_TYPE_PROPERTY, new StringLiteral("SeSSion")));

    // When:
    final CreateStreamCommand cmd = createCmd();

    // Then:
    assertThat(cmd.keySerde,
        is(instanceOf(WindowedSerdes.sessionWindowedSerdeFrom(String.class).getClass())));
  }

  @Test
  public void shouldExtractHoppingWindowType() {
    // Given:
    givenPropertiesWith(ImmutableMap.of(
        DdlConfig.WINDOW_TYPE_PROPERTY, new StringLiteral("HoPPing")));

    // When:
    final CreateStreamCommand cmd = createCmd();

    // Then:
    assertThat(cmd.keySerde,
        is(instanceOf(WindowedSerdes.timeWindowedSerdeFrom(String.class).getClass())));
  }

  @Test
  public void shouldExtractTumblingWindowType() {
    // Given:
    givenPropertiesWith(ImmutableMap.of(
        DdlConfig.WINDOW_TYPE_PROPERTY, new StringLiteral("Tumbling")));

    // When:
    final CreateStreamCommand cmd = createCmd();

    // Then:
    assertThat(cmd.keySerde,
        is(instanceOf(WindowedSerdes.timeWindowedSerdeFrom(String.class).getClass())));
  }

  @Test
  public void shouldThrowOnUnknownWindowType() {
    // Given:
    givenPropertiesWith(ImmutableMap.of(
        DdlConfig.WINDOW_TYPE_PROPERTY, new StringLiteral("Unknown")));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        this::createCmd
    );

    // Then:
    assertThat(e.getMessage(), containsString("WINDOW_TYPE property is not set correctly. "
        + "value: UNKNOWN, validValues: [SESSION, TUMBLING, HOPPING]"));
  }

  @Test
  public void shouldThrowOnOldWindowProperty() {
    // Given:
    givenPropertiesWith(ImmutableMap.of(
        "WINDOWED", new BooleanLiteral("true")));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        this::createCmd
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Invalid config variable in the WITH clause: WINDOWED"));
  }

  @Test
  public void shouldThrowIfTopicDoesNotExist() {
    // Given:
    when(topicClient.isTopicExists(any())).thenReturn(false);

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        this::createCmd
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Kafka topic does not exist: some-topic"));
  }

  private CreateStreamCommand createCmd() {
    return new CreateStreamCommand("some sql", createStreamStatement, topicClient);
  }

  private void givenPropertiesWith(final Map<String, Expression> props) {
    final Map<String, Expression> allProps = new HashMap<>(props);
    allProps.putIfAbsent(DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("Json"));
    allProps.putIfAbsent(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("some-topic"));
    when(createStreamStatement.getProperties()).thenReturn(allProps);
  }
}