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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type.SqlType;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CreateStreamCommandTest {

  private static final List<TableElement> SOME_ELEMENTS = ImmutableList.of(
      new TableElement("bob", PrimitiveType.of(SqlType.STRING)));

  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private CreateStream createStreamStatement;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final MutableMetaStore metaStore = MetaStoreFixture
      .getNewMetaStore(new InternalFunctionRegistry());

  @Before
  public void setUp() {
    givenPropertiesWith((Collections.emptyMap()));
    when(createStreamStatement.getName()).thenReturn(QualifiedName.of("name"));
    when(createStreamStatement.getElements()).thenReturn(SOME_ELEMENTS);
    when(topicClient.isTopicExists(any())).thenReturn(true);
  }

  @Test
  public void shouldDefaultToStringKeySerde() {
    // When:
    final CreateStreamCommand cmd = createCmd();

    // Then:
    assertThat(cmd.keySerdeFactory.create(), is(instanceOf(Serdes.String().getClass())));
  }

  @Test
  public void shouldExtractSessionWindowType() {
    // Given:
    givenPropertiesWith(ImmutableMap.of(
        DdlConfig.WINDOW_TYPE_PROPERTY, new StringLiteral("SeSSion")));

    // When:
    final CreateStreamCommand cmd = createCmd();

    // Then:
    assertThat(cmd.keySerdeFactory.create(),
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
    assertThat(cmd.keySerdeFactory.create(),
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
    assertThat(cmd.keySerdeFactory.create(),
        is(instanceOf(WindowedSerdes.timeWindowedSerdeFrom(String.class).getClass())));
  }

  @Test
  public void shouldThrowOnUnknownWindowType() {
    // Given:
    givenPropertiesWith(ImmutableMap.of(
        DdlConfig.WINDOW_TYPE_PROPERTY, new StringLiteral("Unknown")));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("WINDOW_TYPE property is not set correctly. "
        + "value: UNKNOWN, validValues: [SESSION, TUMBLING, HOPPING]");

    // When:
    createCmd();
  }

  @Test
  public void shouldThrowOnOldWindowProperty() {
    // Given:
    givenPropertiesWith(ImmutableMap.of(
        "WINDOWED", new BooleanLiteral("true")));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Invalid config variable in the WITH clause: WINDOWED");

    // When:
    createCmd();
  }

  @Test
  public void shouldThrowIfTopicDoesNotExist() {
    // Given:
    when(topicClient.isTopicExists(any())).thenReturn(false);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Kafka topic does not exist: some-topic");

    // When:
    createCmd();
  }

  @Test
  public void testCreateAlreadyRegisteredStreamThrowsException() {
    // Given:
    final CreateStreamCommand cmd = createCmd();
    cmd.run(metaStore);

    // Then:
    expectedException.expectMessage("Cannot create stream 'name': A stream " +
            "with name 'name' already exists");

    // When:
    cmd.run(metaStore);
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