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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.ddl.commands.CreateSourceCommand.SerdeOptionsSupplier;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeFactories;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
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

  private static final TableElements ONE_ELEMENT = TableElements.of(
      new TableElement("bob", new Type(SqlTypes.STRING)));

  private static final TableElements SOME_ELEMENTS = TableElements.of(
      new TableElement("bob", new Type(SqlTypes.STRING)),
      new TableElement("hojjat", new Type(SqlTypes.STRING))
  );

  private static final Set<SerdeOption> SOME_SERDE_OPTIONS = ImmutableSet
      .of(SerdeOption.UNWRAP_SINGLE_VALUES);

  private static final QualifiedName SOME_NAME = QualifiedName.of("bob");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private CreateSource statement;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private KsqlTopic topic;
  @Mock
  private SerdeOptionsSupplier serdeOptions;
  @Mock
  private SerdeFactories serdeFactories;
  @Mock
  private KsqlSerdeFactory serdeFactory;
  @Mock
  private MutableMetaStore metaStore;

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

    when(serdeFactories.create(any(), any(CreateSourceProperties.class))).thenReturn(serdeFactory);
  }

  @Test
  public void shouldThrowOnNoElements() {
    // Given:
    when(statement.getElements()).thenReturn(TableElements.of());

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The statement does not define any columns.");

    // When:
    new TestCmd(
        "look mum, no columns",
        statement,
        ksqlConfig,
        kafkaTopicClient,
        serdeOptions,
        serdeFactories
    );
  }

  @Test
  public void shouldNotThrowWhenThereAreElements() {
    // Given:
    when(statement.getElements()).thenReturn(SOME_ELEMENTS);

    // When:
    new TestCmd(
        "look mum, columns",
        statement,
        ksqlConfig,
        kafkaTopicClient,
        serdeOptions,
        serdeFactories
    );

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
    new TestCmd(
        "what, no value topic?",
        statement,
        ksqlConfig,
        kafkaTopicClient,
        serdeOptions,
        serdeFactories
    );
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldNotThrowIfTopicDoesExist() {
    // Given:
    when(kafkaTopicClient.isTopicExists(TOPIC_NAME)).thenReturn(true);

    // When:
    new TestCmd(
        "what, no value topic?",
        statement,
        ksqlConfig,
        kafkaTopicClient,
        serdeOptions,
        serdeFactories
    );

    // Then:
    verify(kafkaTopicClient).isTopicExists(TOPIC_NAME);
  }

  @Test
  public void shouldThrowIfTopicWithSameNameAlreadyRegistered() {
    // Given:
    when(metaStore.getTopic("bob")).thenReturn(topic);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("A topic with name 'bob' already exists");

    // When:
    new TestCmd(
        "topic does exist",
        statement,
        ksqlConfig,
        kafkaTopicClient,
        serdeOptions,
        serdeFactories
    ).registerTopic(metaStore, "topic");
  }

  @Test
  public void shouldRegisterTopic() {
    // Given:
    when(metaStore.getTopic("bob")).thenReturn(null);

    // When:
    new TestCmd(
        "what, no value topic?",
        statement,
        ksqlConfig,
        kafkaTopicClient,
        serdeOptions,
        serdeFactories
    ).registerTopic(metaStore, "topic");

    // Then:
    verify(metaStore).putTopic(argThat(ksqlTopic ->
        ksqlTopic.getKsqlTopicName().equals("bob") &&
        ksqlTopic.getKafkaTopicName().equals(TOPIC_NAME))
    );
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
    new TestCmd(
        "key not in schema!",
        statement,
        ksqlConfig,
        kafkaTopicClient,
        serdeOptions,
        serdeFactories
    );
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
    new TestCmd(
        "key not in schema!",
        statement,
        ksqlConfig,
        kafkaTopicClient,
        serdeOptions,
        serdeFactories
    );
  }

  @Test
  public void shouldBuildSerdeOptions() {
    // Given:
    final CreateStream statement =
        new CreateStream(SOME_NAME, ONE_ELEMENT, true, withProperties);

    final LogicalSchema schema = LogicalSchema.of(SchemaBuilder
        .struct()
        .field("bob", Schema.OPTIONAL_STRING_SCHEMA)
        .build());

    when(serdeOptions.build(any(), any(), any())).thenReturn(SOME_SERDE_OPTIONS);

    // When:
    final TestCmd cmd = new TestCmd(
        "sql",
        statement,
        ksqlConfig,
        kafkaTopicClient,
        serdeOptions,
        serdeFactories
    );

    // Then:
    verify(serdeOptions).build(schema, statement.getProperties(), ksqlConfig);
    assertThat(cmd.getSerdeOptions(), is(SOME_SERDE_OPTIONS));
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
    when(statement.getProperties()).thenReturn(CreateSourceProperties.from(allProps));
  }

  private static final class TestCmd extends CreateSourceCommand {

    private TestCmd(
        final String sqlExpression,
        final CreateSource statement,
        final KsqlConfig ksqlConfig,
        final KafkaTopicClient kafkaTopicClient,
        final SerdeOptionsSupplier serdeOptionsSupplier,
        final SerdeFactories serdeFactories
    ) {
      super(
          sqlExpression,
          statement,
          ksqlConfig,
          kafkaTopicClient,
          serdeOptionsSupplier,
          serdeFactories
      );
    }

    @Override
    public DdlCommandResult run(final MutableMetaStore metaStore) {
      return null;
    }
  }
}