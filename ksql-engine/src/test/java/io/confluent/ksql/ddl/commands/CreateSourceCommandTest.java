/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.CreateSourceProperties;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type.SqlType;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CreateSourceCommandTest {

  private static final String TOPIC_NAME = "some topic";
  private static final List<TableElement> SOME_ELEMENTS = ImmutableList.of(
      new TableElement("bob", PrimitiveType.of(SqlType.STRING)));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();
  @Mock
  private CreateSource statement;
  @Mock
  private KafkaTopicClient kafkaTopicClient;

  @Before
  public void setUp() {
    when(statement.getElements()).thenReturn(SOME_ELEMENTS);
    when(statement.getName()).thenReturn(QualifiedName.of("bob"));
    givenPropertiesWith(ImmutableMap.of());
    when(kafkaTopicClient.isTopicExists(any())).thenReturn(true);
  }

  @Test
  public void shouldThrowOnNoElements() {
    // Given:
    when(statement.getElements()).thenReturn(Collections.emptyList());

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The statement does not define any columns.");

    // When:
    new TestCmd("look mum, no columns", statement, kafkaTopicClient);
  }

  @Test
  public void shouldNotThrowWhenThereAreElements() {
    // Given:
    when(statement.getElements()).thenReturn(SOME_ELEMENTS);

    // When:
    new TestCmd("look mum, columns", statement, kafkaTopicClient);

    // Then: not exception thrown
  }

  @Test
  public void shouldThrowIfTopicDoesNotExist() {
    // Given:
    when(kafkaTopicClient.isTopicExists(any())).thenReturn(false);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Kafka topic does not exist: " + TOPIC_NAME);

    // When:
    new TestCmd("what, no value topic?", statement, kafkaTopicClient);
  }

  @Test
  public void shouldNotThrowIfTopicDoesExist() {
    // Given:
    when(kafkaTopicClient.isTopicExists(TOPIC_NAME)).thenReturn(true);

    // When:
    new TestCmd("what, no value topic?", statement, kafkaTopicClient);

    // Then:
    verify(kafkaTopicClient).isTopicExists(TOPIC_NAME);
  }

  @Test
  public void shouldThrowIfKeyFieldNotInSchema() {
    // Given:
    givenPropertiesWith(ImmutableMap.of(
        DdlConfig.KEY_NAME_PROPERTY, new StringLiteral("will-not-find-me")));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The KEY column set in the WITH clause does not exist in the schema: "
            + "'WILL-NOT-FIND-ME'");

    // When:
    new TestCmd("key not in schema!", statement, kafkaTopicClient);
  }

  @Test
  public void shouldThrowIfTimestampColumnDoesNotExist() {
    // Given:
    givenPropertiesWith(ImmutableMap.of());
    givenPropertiesWith(ImmutableMap.of(
        DdlConfig.TIMESTAMP_NAME_PROPERTY, new StringLiteral("will-not-find-me")));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The TIMESTAMP column set in the WITH clause does not exist in the schema: "
            + "'WILL-NOT-FIND-ME'");

    // When:
    new TestCmd("key not in schema!", statement, kafkaTopicClient);
  }

  private static Map<String, Literal> minValidProps() {
    return ImmutableMap.of(
        DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("json"),
        DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(TOPIC_NAME)
    );
  }

  private static CreateSourceProperties propsWithout(final String name) {
    final HashMap<String, Literal> props = new HashMap<>(minValidProps());
    final Expression removed = props.remove(name);
    assertThat("invalid test", removed, is(notNullValue()));
    return new CreateSourceProperties(ImmutableMap.copyOf(props));
  }

  private void givenPropertiesWith(final Map<String, Literal> additionalProps) {
    final Map<String, Literal> allProps = new HashMap<>(minValidProps());
    allProps.putAll(additionalProps);
    when(statement.getProperties()).thenReturn(new CreateSourceProperties(allProps));
  }

  private static final class TestCmd extends CreateSourceCommand {

    private TestCmd(
        final String sqlExpression,
        final CreateSource statement,
        final KafkaTopicClient kafkaTopicClient
    ) {
      super(sqlExpression, statement, kafkaTopicClient);
    }

    @Override
    public DdlCommandResult run(final MutableMetaStore metaStore) {
      return null;
    }
  }
}