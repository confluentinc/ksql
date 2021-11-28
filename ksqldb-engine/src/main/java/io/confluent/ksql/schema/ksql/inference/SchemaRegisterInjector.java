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

import static io.confluent.ksql.util.KsqlConstants.getSRSubject;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.AbstractKafkaProtobufSerializer;
import io.confluent.kafka.serializers.subject.DefaultReferenceSubjectNameStrategy;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.exception.KsqlSchemaAuthorizationException;
import io.confluent.ksql.execution.ddl.commands.CreateSourceCommand;
import io.confluent.ksql.parser.properties.with.SourcePropertiesUtil;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaAndId;
import io.confluent.ksql.schema.registry.SchemaRegistryUtil;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SchemaTranslator;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.SerdeFeaturesFactory;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlSchemaRegistryNotConfiguredException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.Pair;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.acl.AclOperation;

public class SchemaRegisterInjector implements Injector {

  private final KsqlExecutionContext executionContext;
  private final ServiceContext serviceContext;

  public SchemaRegisterInjector(
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    this.executionContext = Objects.requireNonNull(executionContext, "executionContext");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement
  ) {
    if (statement.getStatement() instanceof CreateAsSelect) {
      registerForCreateAs((ConfiguredStatement<? extends CreateAsSelect>) statement);
    } else if (statement.getStatement() instanceof CreateSource) {
      registerForCreateSource((ConfiguredStatement<? extends CreateSource>) statement);
    }

    return statement;
  }

  private void registerForCreateSource(final ConfiguredStatement<? extends CreateSource> cs) {
    // since this injector is chained after the TopicCreateInjector,
    // we can assume that the kafka topic is always present in the
    // statement properties
    final CreateSource statement = cs.getStatement();
    final LogicalSchema schema = statement.getElements().toLogicalSchema();

    final FormatInfo keyFormat = SourcePropertiesUtil.getKeyFormat(
        statement.getProperties(), statement.getName());
    final SerdeFeatures keyFeatures = SerdeFeaturesFactory.buildKeyFeatures(
        schema,
        FormatFactory.of(keyFormat)
    );

    final FormatInfo valueFormat = SourcePropertiesUtil.getValueFormat(statement.getProperties());
    final SerdeFeatures valFeatures = SerdeFeaturesFactory.buildValueFeatures(
        schema,
        FormatFactory.of(valueFormat),
        statement.getProperties().getValueSerdeFeatures(),
        cs.getSessionConfig().getConfig(false)
    );

    final SchemaAndId rawKeySchema = (SchemaAndId) cs.getSessionConfig().getOverrides()
        .get(CommonCreateConfigs.KEY_SCHEMA_ID);
    final SchemaAndId rawValueSchema = (SchemaAndId) cs.getSessionConfig().getOverrides()
        .get(CommonCreateConfigs.VALUE_SCHEMA_ID);
    final Pair<SchemaAndId, SchemaAndId> kvRawSchema = Pair.of(rawKeySchema, rawValueSchema);

    registerSchemas(
        schema,
        kvRawSchema,
        statement.getProperties().getKafkaTopic(),
        keyFormat,
        keyFeatures,
        valueFormat,
        valFeatures,
        cs.getSessionConfig().getConfig(false),
        cs.getStatementText(),
        false
    );
  }

  private void registerForCreateAs(final ConfiguredStatement<? extends CreateAsSelect> cas) {
    final CreateSourceCommand createSourceCommand;

    try {
      final ServiceContext sandboxServiceContext = SandboxedServiceContext.create(serviceContext);
      createSourceCommand = (CreateSourceCommand)
          executionContext.createSandbox(sandboxServiceContext)
              .plan(sandboxServiceContext, cas)
              .getDdlCommand()
              .get();
    } catch (final Exception e) {
      throw new KsqlStatementException(
          "Could not determine output schema for query due to error: "
              + e.getMessage(), cas.getStatementText(), e);
    }

    registerSchemas(
        createSourceCommand.getSchema(),
        Pair.of(null, null),
        createSourceCommand.getTopicName(),
        createSourceCommand.getFormats().getKeyFormat(),
        createSourceCommand.getFormats().getKeyFeatures(),
        createSourceCommand.getFormats().getValueFormat(),
        createSourceCommand.getFormats().getValueFeatures(),
        cas.getSessionConfig().getConfig(false),
        cas.getStatementText(),
        true
    );
  }

  private void registerSchemas(
      final LogicalSchema schema,
      final Pair<SchemaAndId, SchemaAndId> kvRawSchema,
      final String kafkaTopic,
      final FormatInfo keyFormat,
      final SerdeFeatures keySerdeFeatures,
      final FormatInfo valueFormat,
      final SerdeFeatures valueSerdeFeatures,
      final KsqlConfig config,
      final String statementText,
      final boolean registerIfSchemaExists
  ) {
    if (kvRawSchema.left == null) {
      registerSchema(
          schema.key(),
          kafkaTopic,
          keyFormat,
          keySerdeFeatures,
          config,
          statementText,
          registerIfSchemaExists,
          getSRSubject(kafkaTopic, true),
          true
      );
    } else {
      registerRawSchema(
          kvRawSchema.left,
          kafkaTopic,
          keyFormat,
          config,
          statementText,
          getSRSubject(kafkaTopic, true),
          true);
    }

    if (kvRawSchema.right == null) {
      registerSchema(
          schema.value(),
          kafkaTopic,
          valueFormat,
          valueSerdeFeatures,
          config,
          statementText,
          registerIfSchemaExists,
          getSRSubject(kafkaTopic, false),
          false
      );
    } else {
      registerRawSchema(
          kvRawSchema.left,
          kafkaTopic,
          keyFormat,
          config,
          statementText,
          getSRSubject(kafkaTopic, false),
          false);
    }
  }

  private void registerRawSchema(
      final SchemaAndId schemaAndId,
      final String topic,
      final FormatInfo formatInfo,
      final KsqlConfig config,
      final String statementText,
      final String subject,
      final Boolean isKey
  ) {
    final Format format = FormatFactory.of(formatInfo);
    if (!canRegister(format, config, topic)) {
      throw new KsqlStatementException("Format "
          + format.name() + " doesn't support registering in Schema Registry",
          statementText);
    }

    final SchemaTranslator translator = format.getSchemaTranslator(formatInfo.getProperties());
    if (!translator.name().equals(schemaAndId.rawSchema.schemaType())) {
      final String kvStr = isKey ? "key" : "value";
      throw new KsqlStatementException(String.format(
          "Format and fetched schema type using %s_schema_id %d are different. Format: [%s], "
              + "Fetched schema type: [%s].",
          kvStr, schemaAndId.id, format.name(), translator.name()), statementText);
    }

    final int id = registerRawSchema(serviceContext.getSchemaRegistryClient(),
        schemaAndId.rawSchema, topic, subject, statementText, isKey);

    if (id != schemaAndId.id) {
      final String kvStr = isKey ? "key" : "value";
      throw new KsqlStatementException(
          "Schema id registered is "
              + id
              + " which is different from provided " + kvStr + "_schema_id " + schemaAndId.id + "."
              + System.lineSeparator()
              + "Topic: " + topic
              + System.lineSeparator()
              + "Subject: " + subject
              + System.lineSeparator()
              + "Schema: " + schemaAndId.rawSchema, statementText
      );
    }
  }

  private static int registerRawSchema(
      final SchemaRegistryClient srClient,
      final ParsedSchema parsedSchema,
      final String topic,
      final String subject,
      final String statementText,
      final boolean isKey
  ) {
    try {
      if (parsedSchema instanceof ProtobufSchema) {
        final ProtobufSchema resolved = AbstractKafkaProtobufSerializer.resolveDependencies(
            srClient,
            true,
            false,
            true,
            null,
            new DefaultReferenceSubjectNameStrategy(),
            topic,
            isKey,
            (ProtobufSchema) parsedSchema
        );
        return srClient.register(subject, resolved);
      } else {
        return srClient.register(subject, parsedSchema);
      }
    } catch (IOException | RestClientException e) {
      if (SchemaRegistryUtil.isAuthErrorCode(e)) {
        final AclOperation deniedOperation = SchemaRegistryUtil.getDeniedOperation(e.getMessage());

        if (deniedOperation != AclOperation.UNKNOWN) {
          throw new KsqlSchemaAuthorizationException(
              deniedOperation,
              subject);
        }
      }

      throw new KsqlStatementException(
          "Could not register schema for topic: " + e.getMessage(),
          statementText,
          e);
    }
  }

  private static boolean canRegister(
      final Format format,
      final KsqlConfig config,
      final String topic
  ) {
    if (!format.supportsFeature(SerdeFeature.SCHEMA_INFERENCE)) {
      return false;
    }

    if (config.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY).isEmpty()) {
      throw new KsqlSchemaRegistryNotConfiguredException(
          String.format(
              "Cannot create topic '%s' with format %s without configuring '%s'",
              topic, format.name(), KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)
      );
    }

    return true;
  }

  private void registerSchema(
      final List<? extends SimpleColumn> schema,
      final String topic,
      final FormatInfo formatInfo,
      final SerdeFeatures serdeFeatures,
      final KsqlConfig config,
      final String statementText,
      final boolean registerIfSchemaExists,
      final String subject,
      final boolean isKey
  ) {
    final Format format = FormatFactory.of(formatInfo);
    if (!canRegister(format, config, topic)) {
      return;
    }

    final SchemaRegistryClient srClient = serviceContext.getSchemaRegistryClient();

    if (registerIfSchemaExists || !SchemaRegistryUtil.subjectExists(srClient, subject)) {
      final SchemaTranslator translator = format.getSchemaTranslator(formatInfo.getProperties());

      final ParsedSchema parsedSchema = translator.toParsedSchema(
          PersistenceSchema.from(schema, serdeFeatures)
      );
      registerRawSchema(srClient, parsedSchema, topic, subject, statementText, isKey);
    }
  }
}
