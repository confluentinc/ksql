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

import static io.confluent.ksql.model.WindowType.HOPPING;
import static io.confluent.ksql.model.WindowType.SESSION;
import static io.confluent.ksql.model.WindowType.TUMBLING;
import static io.confluent.ksql.serde.Format.AVRO;
import static io.confluent.ksql.serde.Format.JSON;
import static io.confluent.ksql.serde.Format.KAFKA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.ddl.commands.CreateSourceCommand.SerdeOptionsSupplier;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CreateConfigs;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
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
      new TableElement(Namespace.VALUE, "bob", new Type(SqlTypes.STRING)));

  private static final TableElements SOME_ELEMENTS = TableElements.of(
      new TableElement(Namespace.VALUE, "bob", new Type(SqlTypes.STRING)),
      new TableElement(Namespace.VALUE, "hojjat", new Type(SqlTypes.STRING))
  );

  private static final Set<SerdeOption> SOME_SERDE_OPTIONS = ImmutableSet
      .of(SerdeOption.UNWRAP_SINGLE_VALUES);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private CreateSource statement;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private SerdeOptionsSupplier serdeOptions;
  @Mock
  private ValueSerdeFactory serdeFactories;
  @Mock
  private Serde<GenericRow> serde;

  private KsqlConfig ksqlConfig;


  @Before
  public void setUp() {
    when(statement.getElements()).thenReturn(SOME_ELEMENTS);
    when(statement.getName()).thenReturn(QualifiedName.of("bob"));
    givenPropertiesWith(ImmutableMap.of());
    when(kafkaTopicClient.isTopicExists(any())).thenReturn(true);

    ksqlConfig = new KsqlConfig(ImmutableMap.of());

    when(serdeFactories.create(any(), any(), any(), any(), any(), any())).thenReturn(serde);

    when(serviceContext.getTopicClient()).thenReturn(kafkaTopicClient);
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
    createCmd();
  }

  @Test
  public void shouldNotThrowWhenThereAreElements() {
    // Given:
    when(statement.getElements()).thenReturn(SOME_ELEMENTS);

    // When:
    createCmd();

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
    createCmd();
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldNotThrowIfTopicDoesExist() {
    // Given:
    when(kafkaTopicClient.isTopicExists(TOPIC_NAME)).thenReturn(true);

    // When:
    createCmd();

    // Then:
    verify(kafkaTopicClient).isTopicExists(TOPIC_NAME);
  }

  @Test
  public void shouldThrowIfKeyFieldNotInSchema() {
    // Given:
    givenPropertiesWith(ImmutableMap.of(
        CreateConfigs.KEY_NAME_PROPERTY, new StringLiteral("will-not-find-me")));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The KEY column set in the WITH clause does not exist in the schema: "
            + "'WILL-NOT-FIND-ME'");

    // When:
    createCmd();
  }

  @Test
  public void shouldThrowIfTimestampColumnDoesNotExist() {
    // Given:
    givenPropertiesWith(ImmutableMap.of());
    givenPropertiesWith(ImmutableMap.of(
        CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY, new StringLiteral("will-not-find-me")));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The TIMESTAMP column set in the WITH clause does not exist in the schema: "
            + "'WILL-NOT-FIND-ME'");

    // When:
    createCmd();
  }

  @Test
  public void shouldBuildSerdeOptions() {
    // Given:
    when(statement.getElements()).thenReturn(ONE_ELEMENT);

    final LogicalSchema schema = LogicalSchema.of(SchemaBuilder
        .struct()
        .field("bob", Schema.OPTIONAL_STRING_SCHEMA)
        .build());

    when(serdeOptions.build(any(), any(), any(), any())).thenReturn(SOME_SERDE_OPTIONS);

    // When:
    final TestCmd cmd = createCmd();

    // Then:
    verify(serdeOptions).build(
        schema,
        statement.getProperties().getValueFormat(),
        statement.getProperties().getWrapSingleValues(),
        ksqlConfig
    );
    assertThat(cmd.getSerdeOptions(), is(SOME_SERDE_OPTIONS));
  }

  @Test
  public void shouldBuildSchemaWithImplicitKeyField() {
    // Given:
    when(statement.getElements()).thenReturn(TableElements.of(
        new TableElement(Namespace.VALUE, "bob", new Type(SqlTypes.STRING)),
        new TableElement(Namespace.VALUE, "hojjat", new Type(SqlTypes.STRING))
    ));

    // When:
    final TestCmd result = createCmd();

    // Then:
    assertThat(result.schema, is(LogicalSchema.of(
        SchemaBuilder
            .struct()
            .field("ROWKEY", Schema.OPTIONAL_STRING_SCHEMA)
            .build(),
        SchemaBuilder
            .struct()
            .field("bob", Schema.OPTIONAL_STRING_SCHEMA)
            .field("hojjat", Schema.OPTIONAL_STRING_SCHEMA)
            .build()
    )));
  }

  @Test
  public void shouldBuildSchemaWithExplicitKeyField() {
    // Given:
    when(statement.getElements()).thenReturn(TableElements.of(
        new TableElement(Namespace.KEY, "ROWKEY", new Type(SqlTypes.STRING)),
        new TableElement(Namespace.VALUE, "bob", new Type(SqlTypes.STRING)),
        new TableElement(Namespace.VALUE, "hojjat", new Type(SqlTypes.STRING))
    ));

    // When:
    final TestCmd result = createCmd();

    // Then:
    assertThat(result.schema, is(LogicalSchema.of(
        SchemaBuilder
            .struct()
            .field("ROWKEY", Schema.OPTIONAL_STRING_SCHEMA)
            .build(),
        SchemaBuilder
            .struct()
            .field("bob", Schema.OPTIONAL_STRING_SCHEMA)
            .field("hojjat", Schema.OPTIONAL_STRING_SCHEMA)
            .build()
    )));
  }

  @Test
  public void shouldCreateSerdeToValidateValueFormatCanHandleValueSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.of(SchemaBuilder
        .struct()
        .field("bob", Schema.OPTIONAL_STRING_SCHEMA)
        .field("hojjat", Schema.OPTIONAL_STRING_SCHEMA)
        .build());

    // When:
    createCmd();

    // Then:
    verify(serdeFactories).create(
        FormatInfo.of(JSON, Optional.empty()),
        PersistenceSchema.from(schema.valueSchema(), false),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    );
  }

  @Test
  public void shouldDefaultToKafkaKeySerde() {
    // When:
    final TestCmd cmd = createCmd();

    // Then:
    assertThat(cmd.getTopic().getKeyFormat(), is(KeyFormat.nonWindowed(FormatInfo.of(KAFKA))));
  }

  @Test
  public void shouldHandleValueAvroSchemaName() {
    // Given:
    givenPropertiesWith((ImmutableMap.of(
        "VALUE_FORMAT", new StringLiteral("Avro"),
        "value_avro_schema_full_name", new StringLiteral("full.schema.name")
    )));

    // When:
    final TestCmd cmd = createCmd();

    // Then:
    assertThat(cmd.getTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(AVRO, Optional.of("full.schema.name")))));
  }

  @Test
  public void shouldHandleSessionWindowedKey() {
    // Given:
    givenPropertiesWith((ImmutableMap.of(
        "window_type", new StringLiteral("session")
    )));

    // When:
    final TestCmd cmd = createCmd();

    // Then:
    assertThat(cmd.getTopic().getKeyFormat(), is(KeyFormat.windowed(
        FormatInfo.of(KAFKA),
        WindowInfo.of(SESSION, Optional.empty()))
    ));
  }

  @Test
  public void shouldHandleTumblingWindowedKey() {
    // Given:
    givenPropertiesWith((ImmutableMap.of(
        "window_type", new StringLiteral("tumbling"),
        "window_size", new StringLiteral("1 MINUTE")
    )));

    // When:
    final TestCmd cmd = createCmd();

    // Then:
    assertThat(cmd.getTopic().getKeyFormat(), is(KeyFormat.windowed(
        FormatInfo.of(KAFKA),
        WindowInfo.of(TUMBLING, Optional.of(Duration.ofMinutes(1))))
    ));
  }

  @Test
  public void shouldHandleHoppingWindowedKey() {
    // Given:
    givenPropertiesWith((ImmutableMap.of(
        "window_type", new StringLiteral("Hopping"),
        "window_size", new StringLiteral("2 SECONDS")
    )));

    // When:
    final TestCmd cmd = createCmd();

    // Then:
    assertThat(cmd.getTopic().getKeyFormat(), is(KeyFormat.windowed(
        FormatInfo.of(KAFKA),
        WindowInfo.of(HOPPING, Optional.of(Duration.ofSeconds(2))))
    ));
  }

  private TestCmd createCmd() {
    return new TestCmd(
        "sql",
        statement,
        ksqlConfig,
        serviceContext,
        serdeOptions,
        serdeFactories
    );
  }

  private static Map<String, Literal> minValidProps() {
    return ImmutableMap.of(
        CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("json"),
        CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(TOPIC_NAME)
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
        final ServiceContext serviceContext,
        final SerdeOptionsSupplier serdeOptionsSupplier,
        final ValueSerdeFactory serdeFactories
    ) {
      super(
          sqlExpression,
          statement,
          ksqlConfig,
          serviceContext,
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