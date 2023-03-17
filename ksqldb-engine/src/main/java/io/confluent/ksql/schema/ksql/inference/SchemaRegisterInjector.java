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
import io.confluent.ksql.execution.ddl.commands.CreateSourceCommand;
import io.confluent.ksql.parser.properties.with.SourcePropertiesUtil;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
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
import java.io.IOException;
import java.util.List;
import java.util.Objects;

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

    registerSchemas(
        schema,
        statement.getProperties().getKafkaTopic(),
        keyFormat,
        keyFeatures,
        valueFormat,
        valFeatures,
        cs.getSessionConfig().getConfig(false),
        cs.getMaskedStatementText(),
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
      if (e instanceof KsqlStatementException) {
        throw new KsqlStatementException(
            e.getMessage() == null ? "Server Error" : e.getMessage(),
            ((KsqlStatementException) e).getUnloggedMessage(),
            ((KsqlStatementException) e).getSqlStatement(),
            e
        );
      } else {
        throw new KsqlStatementException(
            "Could not determine output schema for query due to error: "
                + e.getMessage(), cas.getMaskedStatementText(), e);
      }
    }

    registerSchemas(
        createSourceCommand.getSchema(),
        createSourceCommand.getTopicName(),
        createSourceCommand.getFormats().getKeyFormat(),
        createSourceCommand.getFormats().getKeyFeatures(),
        createSourceCommand.getFormats().getValueFormat(),
        createSourceCommand.getFormats().getValueFeatures(),
        cas.getSessionConfig().getConfig(false),
        cas.getMaskedStatementText(),
        true
    );
  }

  private void registerSchemas(
      final LogicalSchema schema,
      final String kafkaTopic,
      final FormatInfo keyFormat,
      final SerdeFeatures keySerdeFeatures,
      final FormatInfo valueFormat,
      final SerdeFeatures valueSerdeFeatures,
      final KsqlConfig config,
      final String statementText,
      final boolean registerIfSchemaExists
  ) {
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
    if (!format.supportsFeature(SerdeFeature.SCHEMA_INFERENCE)) {
      return;
    }

    if (config.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY).isEmpty()) {
      throw new KsqlSchemaRegistryNotConfiguredException(
          String.format(
              "Cannot create topic '%s' with format %s without configuring '%s'",
              topic, format.name(), KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)
      );
    }

    try {
      final SchemaRegistryClient srClient = serviceContext.getSchemaRegistryClient();

      if (registerIfSchemaExists || !SchemaRegistryUtil.subjectExists(srClient, subject)) {
        final SchemaTranslator translator = format.getSchemaTranslator(formatInfo.getProperties());

        final ParsedSchema parsedSchema = translator.toParsedSchema(
            PersistenceSchema.from(schema, serdeFeatures)
        );
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
          srClient.register(subject, resolved);
        } else {
          srClient.register(subject, parsedSchema);
        }
      }
    } catch (IOException | RestClientException e) {
      throw new KsqlStatementException(
          "Could not register schema for topic: " + e.getMessage(),
          statementText,
          e);
    }
  }
}
