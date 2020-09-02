/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.schema.ksql.inference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.KafkaConsumerGroupClient;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlSchemaRegistryNotConfiguredException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchemaRegisterInjectorTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("F1"), SqlTypes.STRING)
      .build();

  private static final AvroSchema AVRO_SCHEMA = new AvroSchema(
      "{\"type\":\"record\",\"name\":\"KsqlDataSourceSchema\","
          + "\"namespace\":\"io.confluent.ksql.avro_schemas\",\"fields\":"
          + "[{\"name\":\"F1\",\"type\":[\"null\",\"string\"],\"default\":null}],"
          + "\"connect.name\":\"io.confluent.ksql.avro_schemas.KsqlDataSourceSchema\"}");

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private SchemaRegistryClient schemaRegistryClient;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private KafkaConsumerGroupClient consumerGroupClient;
  @Mock
  private KsqlExecutionContext executionContext;
  @Mock
  private KsqlExecutionContext executionSandbox;
  @Mock
  private PersistentQueryMetadata queryMetadata;
  @Mock
  private PhysicalSchema physicalSchema;
  @Mock
  private SerdeOptions serdeOptions;

  private final KsqlParser parser = new DefaultKsqlParser();

  private MutableMetaStore metaStore;
  private KsqlConfig config;
  private SchemaRegisterInjector injector;
  private ConfiguredStatement<?> statement;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    config = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "foo:8081"
    ));
    injector = new SchemaRegisterInjector(executionContext, serviceContext);

    when(serviceContext.getSchemaRegistryClient()).thenReturn(schemaRegistryClient);
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(serviceContext.getConsumerGroupClient()).thenReturn(consumerGroupClient);

    when(executionContext.createSandbox(any())).thenReturn(executionSandbox);

    when(queryMetadata.getLogicalSchema()).thenReturn(SCHEMA);
    when(queryMetadata.getResultTopic()).thenReturn(new KsqlTopic(
        "SINK",
        KeyFormat.of(FormatInfo.of(FormatFactory.KAFKA.name()), Optional.empty()),
        ValueFormat.of(FormatInfo.of(FormatFactory.AVRO.name()))
    ));
    when(queryMetadata.getPhysicalSchema()).thenReturn(physicalSchema);

    when(physicalSchema.serdeOptions()).thenReturn(serdeOptions);

    final KsqlTopic sourceTopic = new KsqlTopic(
        "source",
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name())),
        ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()))
    );
    final KsqlStream<?> source = new KsqlStream<>(
        "",
        SourceName.of("SOURCE"),
        SCHEMA,
        SerdeOptions.of(),
        Optional.empty(),
        false,
        sourceTopic
    );
    metaStore.putSource(source, false);
  }

  @Test
  public void shouldNotRegisterSchemaIfSchemaRegistryIsDisabled() {
    // Given:
    config = new KsqlConfig(ImmutableMap.of());
    givenStatement("CREATE STREAM sink (f1 VARCHAR) WITH(kafka_topic='expectedName', value_format='AVRO', partitions=1);");

    // When:
    final KsqlSchemaRegistryNotConfiguredException e = assertThrows(KsqlSchemaRegistryNotConfiguredException.class, () -> injector.inject(statement));

    // Then:
    assertThat(e.getMessage(), containsString("Cannot create topic 'expectedName' with format AVRO without configuring"));
  }

  @Test
  public void shouldNotRegisterSchemaForSchemaRegistryDisabledFormatCreateSource() {
    // Given:
    givenStatement("CREATE STREAM sink (f1 VARCHAR) WITH(kafka_topic='expectedName', value_format='DELIMITED', partitions=1);");

    // When:
    injector.inject(statement);

    // Then:
    verifyNoMoreInteractions(schemaRegistryClient);
  }

  @Test
  public void shouldRegisterSchemaForSchemaRegistryEnabledFormatCreateSourceIfSubjectDoesntExist()
      throws Exception {
    // Given:
    givenStatement("CREATE STREAM sink (f1 VARCHAR) WITH (kafka_topic='expectedName', value_format='AVRO', partitions=1);");

    // When:
    injector.inject(statement);

    // Then:
    verify(schemaRegistryClient).register("expectedName-value", AVRO_SCHEMA);
  }

  @SuppressWarnings("deprecation") // make sure deprecated method is not called
  @Test
  public void shouldNotReplaceExistingSchemaForSchemaRegistryEnabledFormatCreateSource()
      throws Exception {
    // Given:
    givenStatement("CREATE STREAM sink (f1 VARCHAR) WITH (kafka_topic='expectedName', value_format='AVRO', partitions=1);");
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableSet.of("expectedName-value"));

    // When:
    injector.inject(statement);

    // Then:
    verify(schemaRegistryClient, never()).register(any(), any(ParsedSchema.class));
    verify(schemaRegistryClient, never()).register(any(), any(Schema.class));
  }

  @Test
  public void shouldNotRegisterSchemaForSchemaRegistryDisabledFormatCreateAsSelect() {
    // Given:
    config = new KsqlConfig(ImmutableMap.of());
    givenStatement("CREATE STREAM sink WITH(value_format='DELIMITED') AS SELECT * FROM SOURCE;");

    // When:
    final KsqlSchemaRegistryNotConfiguredException e = assertThrows(KsqlSchemaRegistryNotConfiguredException.class, () -> injector.inject(statement));

    // Then:
    assertThat(e.getMessage(), containsString("Cannot create topic 'SINK' with format AVRO without configuring"));
  }

  @Test
  public void shouldRegisterSchemaForSchemaRegistryEnabledFormatCreateAsSelect() throws Exception {
    // Given:
    givenStatement("CREATE STREAM sink WITH(value_format='AVRO') AS SELECT * FROM SOURCE;");

    // When:
    injector.inject(statement);

    // Then:
    verify(schemaRegistryClient).register("SINK-value", AVRO_SCHEMA);
  }

  @Test
  public void shouldPropagateErrorOnFailureToExecuteQuery() {
    // Given:
    givenStatement("CREATE STREAM sink WITH(value_format='AVRO') AS SELECT * FROM SOURCE;");
    when(executionSandbox.execute(any(), eq(statement))).thenReturn(ExecuteResult.of("fail!"));

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> injector.inject(statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Could not determine output schema for query due to error: Optional[fail!]"));
  }

  @Test
  public void shouldPropagateErrorOnSRClientError() throws Exception {
    // Given:
    givenStatement("CREATE STREAM sink WITH(value_format='AVRO') AS SELECT * FROM SOURCE;");
    when(schemaRegistryClient.register(anyString(), any(ParsedSchema.class)))
        .thenThrow(new IOException("FUBAR"));

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> injector.inject(statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Could not register schema for topic"));
    assertThat(e.getCause(), (hasProperty("message", is("FUBAR"))));
  }

  @Test
  public void shouldNotExecuteQueryOnOriginalExecutionContext() {
    // Given:
    givenStatement("CREATE STREAM sink WITH(value_format='AVRO') AS SELECT * FROM SOURCE;");

    // When:
    injector.inject(statement);

    // Then:
    verify(executionContext, Mockito.never()).execute(any(), any(ConfiguredStatement.class));
    verify(executionSandbox, Mockito.times(1)).execute(any(), any(ConfiguredStatement.class));
  }

  @Test
  public void shouldSupportPrimitiveValueSchemasInCreateStmts() throws Exception {
    // Given:
    givenStatement("CREATE STREAM source (f1 VARCHAR) "
        + "WITH ("
        + "  kafka_topic='expectedName', "
        + "  value_format='AVRO', "
        + "  partitions=1, "
        + "  wrap_single_value='false'"
        + ");");

    // When:
    injector.inject(statement);

    // Then:
    verify(schemaRegistryClient)
        .register("expectedName-value", new AvroSchema("{\"type\":\"string\"}"));
  }

  @Test
  public void shouldSupportPrimitiveValueSchemasInCreateAsStmts() throws Exception {
    // Given:
    givenStatement("CREATE STREAM sink "
        + "WITH(value_format='AVRO', wrap_single_value='false') AS "
        + "SELECT * FROM SOURCE;");

    when(serdeOptions.valueWrapping()).thenReturn(Optional.of(SerdeOption.UNWRAP_SINGLE_VALUES));

    // When:
    injector.inject(statement);

    // Then:
    verify(schemaRegistryClient)
        .register("SINK-value", new AvroSchema("{\"type\":\"string\"}"));
  }

  private void givenStatement(final String sql) {
    final PreparedStatement<?> preparedStatement =
        parser.prepare(parser.parse(sql).get(0), metaStore);
    statement = ConfiguredStatement.of(
        preparedStatement,
        new HashMap<>(),
        config);
    when(executionSandbox.execute(any(), eq(statement)))
        .thenReturn(ExecuteResult.of(queryMetadata));
  }
}