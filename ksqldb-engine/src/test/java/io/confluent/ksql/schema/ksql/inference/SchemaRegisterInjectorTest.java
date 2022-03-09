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

import static io.confluent.ksql.properties.with.CommonCreateConfigs.KEY_SCHEMA_ID;
import static io.confluent.ksql.serde.connect.ConnectFormat.VALUE_SCHEMA_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.exception.KsqlSchemaAuthorizationException;
import io.confluent.ksql.execution.ddl.commands.CreateSourceCommand;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaAndId;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.registry.SchemaRegistryUtil;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.KafkaConsumerGroupClient;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlSchemaRegistryNotConfiguredException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.Pair;
import java.io.IOException;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchemaRegisterInjectorTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("ID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("F1"), SqlTypes.STRING)
      .build();

  private static final AvroSchema AVRO_UNWRAPPED_KEY_SCHEMA = new AvroSchema(Schema.create(Type.STRING));
  private static final AvroSchema AVRO_UNWRAPPED_VALUE_SCHEMA = AVRO_UNWRAPPED_KEY_SCHEMA;

  private static final AvroSchema AVRO_SCHEMA = new AvroSchema(
      "{\"type\":\"record\",\"name\":\"KsqlDataSourceSchema\","
          + "\"namespace\":\"io.confluent.ksql.avro_schemas\",\"fields\":"
          + "[{\"name\":\"F1\",\"type\":[\"null\",\"string\"],\"default\":null}],"
          + "\"connect.name\":\"io.confluent.ksql.avro_schemas.KsqlDataSourceSchema\"}");
  private static final ProtobufSchema PROTOBUF_SCHEMA = new ProtobufSchema(
      "syntax = \"proto3\"; import \"google/protobuf/timestamp.proto\";"
          + "message ConnectDefault1 {google.protobuf.Timestamp F1 = 1;}");

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private SchemaRegistryClient schemaRegistryClient;
  @Mock
  private SchemaMetadata schemaMetadata;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private KafkaConsumerGroupClient consumerGroupClient;
  @Mock
  private KsqlExecutionContext executionContext;
  @Mock
  private KsqlExecutionContext executionSandbox;
  @Mock
  private SerdeFeatures keyFeatures;
  @Mock
  private SerdeFeatures valFeatures;
  @Mock
  private KsqlPlan ksqlPlan;
  @Mock
  private CreateSourceCommand ddlCommand;
  @Mock
  private Formats formats;

  private final KsqlParser parser = new DefaultKsqlParser();

  private MutableMetaStore metaStore;
  private KsqlConfig config;
  private SchemaRegisterInjector injector;
  private ConfiguredStatement<?> statement;

  @Before
  public void setUp() throws IOException, RestClientException {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    config = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "foo:8081"
    ));
    injector = new SchemaRegisterInjector(executionContext, serviceContext);

    when(serviceContext.getSchemaRegistryClient()).thenReturn(schemaRegistryClient);
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(serviceContext.getConsumerGroupClient()).thenReturn(consumerGroupClient);

    when(executionContext.createSandbox(any())).thenReturn(executionSandbox);

    when(keyFeatures.enabled(SerdeFeature.UNWRAP_SINGLES)).thenReturn(true);

    when(ddlCommand.getSchema()).thenReturn(SCHEMA);
    when(ddlCommand.getTopicName()).thenReturn("SINK");
    when(ddlCommand.getFormats()).thenReturn(formats);

    when(formats.getKeyFormat()).thenReturn(FormatInfo.of(FormatFactory.AVRO.name()));
    when(formats.getKeyFeatures()).thenReturn(keyFeatures);
    when(formats.getValueFormat()).thenReturn(FormatInfo.of(FormatFactory.AVRO.name()));
    when(formats.getValueFeatures()).thenReturn(valFeatures);

    when(schemaRegistryClient.getLatestSchemaMetadata(any())).thenThrow(
        new RestClientException("foo", 404, SchemaRegistryUtil.SUBJECT_NOT_FOUND_ERROR_CODE));

    final KsqlTopic sourceTopic = new KsqlTopic(
        "source",
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of()),
        ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()), valFeatures)
    );
    final KsqlStream<?> source = new KsqlStream<>(
        "",
        SourceName.of("SOURCE"),
        SCHEMA,
        Optional.empty(),
        false,
        sourceTopic,
        false
    );
    metaStore.putSource(source, false);
  }

  @After
  public void after() throws IOException, RestClientException {
    // we should never call getAllSubjects() because this has stricter
    // privilege requirements (i.e. I may have permission to see subject
    // X but not all subjects)
    verify(schemaRegistryClient, never()).getAllSubjects();
  }

  @Test
  public void shouldNotRegisterSchemaIfSchemaRegistryIsDisabled() {
    // Given:
    config = new KsqlConfig(ImmutableMap.of());
    givenStatement("CREATE STREAM sink (f1 VARCHAR) WITH(kafka_topic='expectedName', key_format='AVRO', value_format='AVRO', partitions=1);");

    // When:
    final KsqlSchemaRegistryNotConfiguredException e = assertThrows(KsqlSchemaRegistryNotConfiguredException.class, () -> injector.inject(statement));

    // Then:
    assertThat(e.getMessage(), containsString("Cannot create topic 'expectedName' with format AVRO without configuring"));
  }

  @Test
  public void shouldNotRegisterSchemaForSchemaRegistryDisabledFormatCreateSource() {
    // Given:
    givenStatement("CREATE STREAM sink (f1 VARCHAR) WITH(kafka_topic='expectedName', key_format='KAFKA', value_format='DELIMITED', partitions=1);");

    // When:
    injector.inject(statement);

    // Then:
    verifyNoMoreInteractions(schemaRegistryClient);
  }

  @Test
  public void shouldRegisterKeySchemaForSchemaRegistryEnabledFormatCreateSourceIfSubjectDoesntExist()
      throws Exception {
    // Given:
    givenStatement("CREATE STREAM sink (id VARCHAR KEY, f1 VARCHAR) WITH (kafka_topic='expectedName', key_format='AVRO', value_format='DELIMITED', partitions=1);");

    // When:
    injector.inject(statement);

    // Then:
    verify(schemaRegistryClient).register("expectedName-key", AVRO_UNWRAPPED_KEY_SCHEMA);
  }

  @Test
  public void shouldRegisterValueSchemaForSchemaRegistryEnabledFormatCreateSourceIfSubjectDoesntExist()
      throws Exception {
    // Given:
    givenStatement("CREATE STREAM sink (f1 VARCHAR) WITH (kafka_topic='expectedName', key_format='KAFKA', value_format='AVRO', partitions=1);");

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
    givenStatement("CREATE STREAM sink (f1 VARCHAR) WITH (kafka_topic='expectedName', key_format='AVRO', value_format='AVRO', partitions=1);");
    doReturn(schemaMetadata).when(schemaRegistryClient).getLatestSchemaMetadata("expectedName-value");
    doReturn(schemaMetadata).when(schemaRegistryClient).getLatestSchemaMetadata("expectedName-key");

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
    givenStatement("CREATE STREAM sink WITH(key_format='AVRO', value_format='AVRO') AS SELECT * FROM SOURCE;");

    // When:
    injector.inject(statement);

    // Then:
    verify(schemaRegistryClient).register("SINK-key", AVRO_UNWRAPPED_KEY_SCHEMA);
    verify(schemaRegistryClient).register("SINK-value", AVRO_SCHEMA);
  }

  @Test
  public void shouldPropagateErrorOnFailureToPlanQuery() {
    // Given:
    givenStatement("CREATE STREAM sink WITH(value_format='AVRO') AS SELECT * FROM SOURCE;");
    doThrow(new KsqlException("fail!")).when(executionSandbox).plan(any(), eq(statement));

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> injector.inject(statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Could not determine output schema for query due to error: fail!"));
  }

  @Test
  public void shouldThrowAuthorizationException() throws Exception {
    // Given:
    givenStatement("CREATE STREAM sink WITH(value_format='AVRO') AS SELECT * FROM SOURCE;");
    when(schemaRegistryClient.register(anyString(), any(ParsedSchema.class)))
        .thenThrow(new RestClientException(
            "User is denied operation Write on Subject", 403, 40301));

    // When:
    final Exception e = assertThrows(
        KsqlSchemaAuthorizationException.class,
        () -> injector.inject(statement)
    );

    // Then:
    assertThat(e.getMessage(), equalTo(
        "Authorization denied to Write on Schema Registry subject: [SINK-key]"));
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
    assertThat(e.getCause(), (hasProperty("message",
        is("Could not register schema for topic: FUBAR"))));
  }

  @Test
  public void shouldNotPlanQueryOnOriginalExecutionContext() {
    // Given:
    givenStatement("CREATE STREAM sink WITH(value_format='AVRO') AS SELECT * FROM SOURCE;");

    // When:
    injector.inject(statement);

    // Then:
    verify(executionContext, Mockito.never()).plan(any(), any(ConfiguredStatement.class));
    verify(executionSandbox, Mockito.times(1))
        .plan(any(), any(ConfiguredStatement.class));
  }

  @Test
  public void shouldSupportPrimitiveValueSchemasInCreateStmts() throws Exception {
    // Given:
    givenStatement("CREATE STREAM source (f1 VARCHAR) "
        + "WITH ("
        + "  kafka_topic='expectedName', "
        + "  key_format='KAFKA', "
        + "  value_format='AVRO', "
        + "  partitions=1, "
        + "  wrap_single_value='false'"
        + ");");

    // When:
    injector.inject(statement);

    // Then:
    verify(schemaRegistryClient).register("expectedName-value", AVRO_UNWRAPPED_VALUE_SCHEMA);
  }

  @Test
  public void shouldSupportPrimitiveValueSchemasInCreateAsStmts() throws Exception {
    // Given:
    givenStatement("CREATE STREAM sink "
        + "WITH(value_format='AVRO', wrap_single_value='false') AS "
        + "SELECT * FROM SOURCE;");

    when(valFeatures.enabled(SerdeFeature.UNWRAP_SINGLES)).thenReturn(true);

    // When:
    injector.inject(statement);

    // Then:
    verify(schemaRegistryClient).register("SINK-value", AVRO_UNWRAPPED_VALUE_SCHEMA);
  }

  @Test
  public void shouldRegisterDependenciesForProtobuf() throws Exception {
    // Given:
    givenStatement("CREATE STREAM source (f1 TIMESTAMP) "
        + "WITH ("
        + "  kafka_topic='expectedName', "
        + "  key_format='KAFKA', "
        + "  value_format='PROTOBUF', "
        + "  partitions=1 "
        + ");");

    // When:
    injector.inject(statement);

    // Then:
    verify(schemaRegistryClient).register("expectedName-value", PROTOBUF_SCHEMA);
  }

  @Test
  public void shouldThrowWrongValueFormatExceptionWithOverrideSchema() {
    // Given:
    final SchemaAndId schemaAndId = SchemaAndId.schemaAndId(SCHEMA.value(), AVRO_SCHEMA, 1);
    givenStatement("CREATE STREAM source (id int key, f1 varchar) "
        + "WITH ("
        + "kafka_topic='expectedName', "
        + "key_format='KAFKA', "
        + "value_format='JSON', "
        + "value_schema_id=1, "
        + "partitions=1"
        + ");", Pair.of(null, schemaAndId));

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> injector.inject(statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "VALUE_SCHEMA_ID is provided but format JSON doesn't "
            + "support registering in Schema Registry"));
  }

  @Test
  public void shouldThrowWrongKeyFormatExceptionWithOverrideSchema() throws Exception {
    // Given:
    final SchemaAndId keySchemaAndId = SchemaAndId.schemaAndId(SCHEMA.value(), AVRO_SCHEMA, 1);
    final SchemaAndId valueSchemaAndId = SchemaAndId.schemaAndId(SCHEMA.value(), AVRO_SCHEMA, 2);
    givenStatement("CREATE STREAM source (id int key, f1 varchar) "
        + "WITH ("
        + "kafka_topic='expectedName', "
        + "key_format='KAFKA', "
        + "value_format='AVRO', "
        + "key_schema_id=1, "
        + "value_schema_id=1, "
        + "partitions=1"
        + ");", Pair.of(keySchemaAndId, valueSchemaAndId));

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> injector.inject(statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "KEY_SCHEMA_ID is provided but format KAFKA doesn't "
            + "support registering in Schema Registry"));
    verify(schemaRegistryClient, never()).register(anyString(), any(ParsedSchema.class));
  }

  @Test
  public void shouldThrowInconsistentKeySchemaTypeExceptionWithOverrideSchema() {
    // Given:
    final SchemaAndId schemaAndId = SchemaAndId.schemaAndId(SCHEMA.value(), AVRO_SCHEMA, 1);
    givenStatement("CREATE STREAM source (id int key, f1 varchar) "
        + "WITH ("
        + "kafka_topic='expectedName', "
        + "key_format='PROTOBUF', "
        + "value_format='JSON', "
        + "key_schema_id=1, "
        + "partitions=1"
        + ");", Pair.of(schemaAndId, null));

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> injector.inject(statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Format and fetched schema type using "
        + "KEY_SCHEMA_ID 1 are different. Format: [PROTOBUF], Fetched schema type: [AVRO]."));
  }

  @Test
  public void shouldThrowInconsistentValueSchemaTypeExceptionWithOverrideSchema() {
    // Given:
    final SchemaAndId schemaAndId = SchemaAndId.schemaAndId(SCHEMA.value(), AVRO_SCHEMA, 1);
    givenStatement("CREATE STREAM source (id int key, f1 varchar) "
        + "WITH ("
        + "kafka_topic='expectedName', "
        + "key_format='PROTOBUF', "
        + "value_format='PROTOBUF', "
        + "value_schema_id=1, "
        + "partitions=1"
        + ");", Pair.of(null, schemaAndId));

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> injector.inject(statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Format and fetched schema type using "
        + "VALUE_SCHEMA_ID 1 are different. Format: [PROTOBUF], Fetched schema type: [AVRO]."));
  }

  @Test
  public void shouldThrowInconsistentSchemaIdExceptionWithOverrideSchema()
      throws IOException, RestClientException {
    // Given:
    when(schemaRegistryClient.register(anyString(), any(ParsedSchema.class))).thenReturn(2);
    final SchemaAndId schemaAndId = SchemaAndId.schemaAndId(SCHEMA.value(), AVRO_SCHEMA, 1);
    givenStatement("CREATE STREAM source (id int key, f1 varchar) "
        + "WITH ("
        + "kafka_topic='expectedName', "
        + "key_format='PROTOBUF', "
        + "value_format='AVRO', "
        + "value_schema_id=1, "
        + "partitions=1"
        + ");", Pair.of(null, schemaAndId));

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> injector.inject(statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Schema id registered is 2 which is "
        + "different from provided VALUE_SCHEMA_ID 1."
        + System.lineSeparator()
        + "Topic: expectedName"
        + System.lineSeparator()
        + "Subject: expectedName-value"));
  }

  @Test
  public void shouldRegisterKeyOverrideSchemaAvro()
      throws IOException, RestClientException {
    // Given:
    when(schemaRegistryClient.register(anyString(), any(ParsedSchema.class))).thenReturn(1);
    final SchemaAndId schemaAndId = SchemaAndId.schemaAndId(SCHEMA.value(), AVRO_SCHEMA, 1);
    givenStatement("CREATE STREAM source (id int key, f1 varchar) "
        + "WITH ("
        + "kafka_topic='expectedName', "
        + "key_format='AVRO', "
        + "value_format='JSON', "
        + "key_schema_id=1, "
        + "partitions=1"
        + ");", Pair.of(schemaAndId, null));

    // When:
    injector.inject(statement);

    // Then:
    verify(schemaRegistryClient).register("expectedName-key", AVRO_SCHEMA);
  }

  @Test
  public void shouldRegisterKeyOverrideSchemaAvroForCreateAs()
      throws IOException, RestClientException {
    // Given:
    when(schemaRegistryClient.register(anyString(), any(ParsedSchema.class))).thenReturn(1);
    final SchemaAndId keySchemaAndId = SchemaAndId.schemaAndId(SCHEMA.value(), AVRO_SCHEMA, 1);
    final SchemaAndId valueSchemaAndId = SchemaAndId.schemaAndId(SCHEMA.value(), AVRO_SCHEMA, 1);
    givenStatement("CREATE STREAM sink WITH (key_schema_id=1, value_schema_id=1, partitions=1"
        + ") AS SELECT * FROM SOURCE;", Pair.of(keySchemaAndId, valueSchemaAndId));

    // When:
    injector.inject(statement);

    // Then:
    verify(schemaRegistryClient).register("SINK-key", AVRO_SCHEMA);
    verify(schemaRegistryClient).register("SINK-value", AVRO_SCHEMA);
  }

  @Test
  public void shouldRegisterValueOverrideSchemaProtobuf()
      throws IOException, RestClientException {
    // Given:
    when(schemaRegistryClient.register(anyString(), any(ParsedSchema.class))).thenReturn(1);
    final SchemaAndId schemaAndId = SchemaAndId.schemaAndId(SCHEMA.value(), PROTOBUF_SCHEMA, 1);
    givenStatement("CREATE STREAM source (id int key, f1 varchar) "
        + "WITH ("
        + "kafka_topic='expectedName', "
        + "key_format='JSON', "
        + "value_format='PROTOBUF', "
        + "value_schema_id=1, "
        + "partitions=1"
        + ");", Pair.of(null, schemaAndId));

    // When:
    injector.inject(statement);

    // Then:
    verify(schemaRegistryClient).register("expectedName-value", PROTOBUF_SCHEMA);
  }

  @Test
  public void shouldRegisterKeyValueOverrideSchema()
      throws IOException, RestClientException {
    // Given:
    when(schemaRegistryClient.register(anyString(), any(AvroSchema.class))).thenReturn(1);
    when(schemaRegistryClient.register(anyString(), any(ProtobufSchema.class))).thenReturn(2);
    final SchemaAndId keySchemaAndId = SchemaAndId.schemaAndId(SCHEMA.value(), AVRO_UNWRAPPED_KEY_SCHEMA, 1);
    final SchemaAndId ValueSchemaAndId = SchemaAndId.schemaAndId(SCHEMA.value(), PROTOBUF_SCHEMA, 2);
    givenStatement("CREATE STREAM source (id int key, f1 varchar) "
        + "WITH ("
        + "kafka_topic='expectedName', "
        + "key_format='AVRO', "
        + "value_format='PROTOBUF', "
        + "value_schema_id=2, "
        + "key_schema_id=1, "
        + "partitions=1"
        + ");", Pair.of(keySchemaAndId, ValueSchemaAndId));

    // When:
    ConfiguredStatement<?> newStatement = injector.inject(statement);

    // Then:
    verify(schemaRegistryClient).register("expectedName-key", AVRO_UNWRAPPED_KEY_SCHEMA);
    verify(schemaRegistryClient).register("expectedName-value", PROTOBUF_SCHEMA);
    assertTrue(statement.getSessionConfig().getOverrides().containsKey(KEY_SCHEMA_ID));
    assertTrue(statement.getSessionConfig().getOverrides().containsKey(VALUE_SCHEMA_ID));
    assertFalse(newStatement.getSessionConfig().getOverrides().containsKey(KEY_SCHEMA_ID));
    assertFalse(newStatement.getSessionConfig().getOverrides().containsKey(VALUE_SCHEMA_ID));
    assertThat(newStatement.getSessionConfig().getOverrides(), hasKey("Dummy key"));
  }

  private void givenStatement(final String sql) {
    givenStatement(sql, Pair.of(null, null));
  }

  private void givenStatement(
      final String sql,
      final Pair<SchemaAndId, SchemaAndId> rawSchemaAndId
  ) {
    final PreparedStatement<?> preparedStatement =
        parser.prepare(parser.parse(sql).get(0), metaStore);

    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    if (rawSchemaAndId.left != null) {
      builder.put(KEY_SCHEMA_ID, rawSchemaAndId.left);
    }
    if (rawSchemaAndId.right != null) {
      builder.put(CommonCreateConfigs.VALUE_SCHEMA_ID, rawSchemaAndId.right);
    }
    builder.put("Dummy key", 1);

    statement = ConfiguredStatement.of(
        preparedStatement,
        SessionConfig.of(config, builder.build())
    );
    when(executionSandbox.plan(any(), eq(statement)))
        .thenReturn(ksqlPlan);
    when(ksqlPlan.getDdlCommand())
        .thenReturn(Optional.of(ddlCommand));
  }
}