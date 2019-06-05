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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.schema.SqlType;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
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

  private static final List<TableElement> ONE_ELEMENT = ImmutableList.of(
      new TableElement("bob", PrimitiveType.of(SqlType.STRING)));

  private static final List<TableElement> SOME_ELEMENTS = ImmutableList.of(
      new TableElement("bob", PrimitiveType.of(SqlType.STRING)),
      new TableElement("hojjat", PrimitiveType.of(SqlType.STRING))
  );

  private static final QualifiedName SOME_NAME = QualifiedName.of("bob");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private CreateSource statement;
  @Mock
  private KafkaTopicClient kafkaTopicClient;

  private KsqlConfig ksqlConfig;
  private final Map<String, Literal> withProperties = new HashMap<>();


  @Before
  public void setUp() {
    when(statement.getElements()).thenReturn(SOME_ELEMENTS);
    when(statement.getName()).thenReturn(QualifiedName.of("bob"));
    givenPropertiesWith(ImmutableMap.of());
    when(kafkaTopicClient.isTopicExists(any())).thenReturn(true);

    ksqlConfig = new KsqlConfig(ImmutableMap.of());

    withProperties.clear();
    withProperties.put(DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("JSON"));
    withProperties.put(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic"));
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
    new TestCmd("look mum, no columns", statement, ksqlConfig, kafkaTopicClient);
  }

  @Test
  public void shouldNotThrowWhenThereAreElements() {
    // Given:
    when(statement.getElements()).thenReturn(SOME_ELEMENTS);

    // When:
    new TestCmd("look mum, columns", statement, ksqlConfig, kafkaTopicClient);

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
    new TestCmd("what, no value topic?", statement, ksqlConfig, kafkaTopicClient);
  }

  @Test
  public void shouldNotThrowIfTopicDoesExist() {
    // Given:
    when(kafkaTopicClient.isTopicExists(TOPIC_NAME)).thenReturn(true);

    // When:
    new TestCmd("what, no value topic?", statement, ksqlConfig, kafkaTopicClient);

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
    new TestCmd("key not in schema!", statement, ksqlConfig, kafkaTopicClient);
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
    new TestCmd("key not in schema!", statement, ksqlConfig, kafkaTopicClient);
  }

  @Test
  public void shouldGetSingleValueWrappingFromPropertiesBeforeConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    withProperties.put(DdlConfig.WRAP_SINGLE_VALUE, new BooleanLiteral("false"));

    final CreateStream statement =
        new CreateStream(SOME_NAME, ONE_ELEMENT, true, withProperties);

    // When:
    final TestCmd cmd = new TestCmd("sql", statement, ksqlConfig, kafkaTopicClient);

    // Then:
    assertThat(cmd.getSerdeOptions(), is(SerdeOption.of(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldGetSingleValueWrappingFromConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    final CreateStream statement =
        new CreateStream(SOME_NAME, ONE_ELEMENT, true, withProperties);

    // When:
    final TestCmd cmd = new TestCmd("sql", statement, ksqlConfig, kafkaTopicClient);

    // Then:
    assertThat(cmd.getSerdeOptions(), is(SerdeOption.of(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldGetSingleValueWrappingFromDefaultConfig() {
    // Given:
    final CreateStream statement =
        new CreateStream(SOME_NAME, ONE_ELEMENT, true, withProperties);

    // When:
    final TestCmd cmd = new TestCmd("sql", statement, ksqlConfig, kafkaTopicClient);

    // Then:
    assertThat(cmd.getSerdeOptions(), is(SerdeOption.none()));
  }

  @Test
  public void shouldNotGetSingleValueWrappingFromConfigForMultiFields() {
    // Given:
    final CreateStream statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final TestCmd cmd = new TestCmd("sql", statement, ksqlConfig, kafkaTopicClient);

    // Then:
    assertThat(cmd.getSerdeOptions(), is(SerdeOption.none()));
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForMultiField() {
    // Given:
    withProperties.put(DdlConfig.WRAP_SINGLE_VALUE, new BooleanLiteral("false"));

    final CreateStream statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "'WRAP_SINGLE_VALUE' is only valid for single-field value schemas");

    // When:
    new TestCmd("sql", statement, ksqlConfig, kafkaTopicClient);
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForDelimited() {
    // Given:
    withProperties.put(DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral(Format.DELIMITED.name()));
    withProperties.put(DdlConfig.WRAP_SINGLE_VALUE, new BooleanLiteral("false"));

    final CreateStream statement =
        new CreateStream(SOME_NAME, ONE_ELEMENT, true, withProperties);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "'WRAP_SINGLE_VALUE' can not be used with format 'DELIMITED' as it does not support wrapping");

    // When:
    new TestCmd("sql", statement, ksqlConfig, kafkaTopicClient);
  }

  private static Map<String, Literal> minValidProps() {
    return ImmutableMap.of(
        DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("json"),
        DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(TOPIC_NAME)
    );
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
        final KsqlConfig ksqlConfig,
        final KafkaTopicClient kafkaTopicClient
    ) {
      super(sqlExpression, statement, ksqlConfig, kafkaTopicClient);
    }

    @Override
    public DdlCommandResult run(final MutableMetaStore metaStore) {
      return null;
    }
  }
}