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
import static io.confluent.ksql.properties.with.CommonCreateConfigs.VALUE_SCHEMA_ID;
import static io.confluent.ksql.util.KsqlConstants.getSRSubject;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext;
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
import io.confluent.ksql.util.ErrorMessageUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlSchemaRegistryNotConfiguredException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.Pair;
import java.util.List;
import java.util.Map;
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
    try {
      if (statement.getStatement() instanceof CreateAsSelect) {
        registerForCreateAs((ConfiguredStatement<? extends CreateAsSelect>) statement);
      } else if (statement.getStatement() instanceof CreateSource) {
        registerForCreateSource((ConfiguredStatement<? extends CreateSource>) statement);
      }
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final KsqlException e) {
      throw new KsqlStatementException(
          ErrorMessageUtil.buildErrorMessage(e),
          statement.getMaskedStatementText(),
          e.getCause());
    }
    // Remove schema id from SessionConfig
    return stripSchemaIdConfig(statement);
  }

  private <T extends Statement> ConfiguredStatement<T> stripSchemaIdConfig(
      final ConfiguredStatement<T> statement) {
    final Map<String, ?> overrides = statement.getSessionConfig().getOverrides();
    if (!overrides.containsKey(KEY_SCHEMA_ID) && !overrides.containsKey(VALUE_SCHEMA_ID)) {
      return statement;
    }
    final ImmutableMap<String, ?> newOverrides = overrides
        .entrySet()
        .stream()
        .filter(e -> !e.getKey().equals(KEY_SCHEMA_ID) && !e.getKey().equals(VALUE_SCHEMA_ID))
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

    return ConfiguredStatement.of(
        statement.getPreparedStatement(),
        statement.getSessionConfig().withNewOverrides(newOverrides)
    );
  }

  private void registerForCreateSource(final ConfiguredStatement<? extends CreateSource> cs) {
    // since this injector is chained after the TopicCreateInjector,
    // we can assume that the kafka topic is always present in the
    // statement properties
    final CreateSource statement = cs.getStatement();
    final LogicalSchema schema = statement.getElements().toLogicalSchema();

    final FormatInfo keyFormatInfo = SourcePropertiesUtil.getKeyFormat(
        statement.getProperties(), statement.getName());
    final Format keyFormat = tryGetFormat(keyFormatInfo, true, cs.getMaskedStatementText());
    final SerdeFeatures keyFeatures = SerdeFeaturesFactory.buildKeyFeatures(
        schema,
        keyFormat
    );

    final FormatInfo valueFormatInfo = SourcePropertiesUtil.getValueFormat(
        statement.getProperties());
    final Format valueFormat = tryGetFormat(valueFormatInfo, false, cs.getMaskedStatementText());
    final SerdeFeatures valFeatures = SerdeFeaturesFactory.buildValueFeatures(
        schema,
        valueFormat,
        statement.getProperties().getValueSerdeFeatures(),
        cs.getSessionConfig().getConfig(false)
    );

    final SchemaAndId rawKeySchema = (SchemaAndId) cs.getSessionConfig().getOverrides()
        .get(KEY_SCHEMA_ID);
    final SchemaAndId rawValueSchema = (SchemaAndId) cs.getSessionConfig().getOverrides()
        .get(CommonCreateConfigs.VALUE_SCHEMA_ID);

    registerSchemas(
        schema,
        Pair.of(rawKeySchema, rawValueSchema),
        statement.getProperties().getKafkaTopic(),
        keyFormatInfo,
        keyFeatures,
        valueFormatInfo,
        valFeatures,
        cs.getSessionConfig().getConfig(false),
        cs.getMaskedStatementText(),
        false
    );
  }

  private static Format tryGetFormat(
      final FormatInfo formatInfo,
      final boolean isKey,
      final String statementText
  ) {
    try {
      return FormatFactory.of(formatInfo);
    } catch (KsqlException e) {
      if (e.getMessage().contains("does not support the following configs: [schemaId]")) {
        final String idMsg =
            isKey ? KEY_SCHEMA_ID : CommonCreateConfigs.VALUE_SCHEMA_ID;
        throw new KsqlStatementException(
            idMsg + " is provided but format " + formatInfo.getFormat()
                + " doesn't support registering in Schema Registry",
            statementText, e);
      }
      throw e;
    }
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
    } catch (final KsqlStatementException e) {
      throw new KsqlStatementException(
          "Could not determine output schema for query due to error: " + e.getMessage(),
          "Could not determine output schema for query due to error: " + e.getUnloggedMessage(),
          cas.getMaskedStatementText(),
          e
      );
    } catch (final Exception e) {
      throw new KsqlStatementException(
          "Could not determine output schema for query due to error: "
              + e.getMessage(), cas.getMaskedStatementText(), e);
    }

    final SchemaAndId rawKeySchema = (SchemaAndId) cas.getSessionConfig().getOverrides()
        .get(KEY_SCHEMA_ID);
    final SchemaAndId rawValueSchema = (SchemaAndId) cas.getSessionConfig().getOverrides()
        .get(CommonCreateConfigs.VALUE_SCHEMA_ID);

    registerSchemas(
        createSourceCommand.getSchema(),
        Pair.of(rawKeySchema, rawValueSchema),
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
    final boolean registerRawKey = kvRawSchema.left != null;
    final boolean registerRawValue = kvRawSchema.right != null;

    if (!registerRawKey) {
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
    }
    if (!registerRawValue) {
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

    // Do all sanity checks before register anything
    if (registerRawKey) {
      // validate
      sanityCheck(kvRawSchema.left, keyFormat, kafkaTopic, config, statementText, true);
    }
    if (registerRawValue) {
      // validate
      sanityCheck(kvRawSchema.right, valueFormat, kafkaTopic, config, statementText, false);
    }
    if (registerRawKey) {
      registerRawSchema(
          kvRawSchema.left,
          kafkaTopic,
          statementText,
          getSRSubject(kafkaTopic, true),
          true);
    }
    if (registerRawValue) {
      registerRawSchema(
          kvRawSchema.right,
          kafkaTopic,
          statementText,
          getSRSubject(kafkaTopic, false),
          false);
    }
  }

  private static void sanityCheck(
      final SchemaAndId schemaAndId,
      final FormatInfo formatInfo,
      final String topic,
      final KsqlConfig config,
      final String statementText,
      final boolean isKey
  ) {
    final String schemaIdPropStr =
        isKey ? KEY_SCHEMA_ID : CommonCreateConfigs.VALUE_SCHEMA_ID;
    final Format format = FormatFactory.of(formatInfo);
    if (!canRegister(format, config, topic)) {
      throw new KsqlStatementException(schemaIdPropStr + " is provided but format "
          + format.name() + " doesn't support registering in Schema Registry",
          statementText);
    }

    final SchemaTranslator translator = format.getSchemaTranslator(formatInfo.getProperties());
    if (!translator.name().equals(schemaAndId.rawSchema.schemaType())) {
      throw new KsqlStatementException(String.format(
          "Format and fetched schema type using %s %d are different. Format: [%s], "
              + "Fetched schema type: [%s].",
          schemaIdPropStr, schemaAndId.id, format.name(), schemaAndId.rawSchema.schemaType()),
          statementText);
    }
  }

  private void registerRawSchema(
      final SchemaAndId schemaAndId,
      final String topic,
      final String statementText,
      final String subject,
      final Boolean isKey
  ) {
    final int id;
    try {
      id = SchemaRegistryUtil.registerSchema(serviceContext.getSchemaRegistryClient(),
          schemaAndId.rawSchema, topic, subject, isKey);
    } catch (KsqlException e) {
      throw new KsqlStatementException("Could not register schema for topic: " + e.getMessage(),
          statementText, e);
    }

    final boolean isSandbox = serviceContext instanceof SandboxedServiceContext;
    // Skip checking sandbox client since sandbox client
    // will return fixed id when register is called.
    if (!isSandbox && id != schemaAndId.id) {
      final String schemaIdPropStr =
          isKey ? KEY_SCHEMA_ID : CommonCreateConfigs.VALUE_SCHEMA_ID;
      throw new KsqlStatementException(
          "Schema id registered is "
              + id
              + " which is different from provided " + schemaIdPropStr + " " + schemaAndId.id + "."
              + System.lineSeparator()
              + "Topic: " + topic
              + System.lineSeparator()
              + "Subject: " + subject
              + System.lineSeparator()
              + "Schema: " + schemaAndId.rawSchema, statementText
      );
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
      try {
        final SchemaTranslator translator = format.getSchemaTranslator(formatInfo.getProperties());
        final ParsedSchema parsedSchema = translator.toParsedSchema(
            PersistenceSchema.from(schema, serdeFeatures)
        );
        SchemaRegistryUtil.registerSchema(srClient, parsedSchema, topic, subject, isKey);
      } catch (KsqlException e) {
        throw new KsqlStatementException("Could not register schema for topic: " + e.getMessage(),
            statementText, e);
      }
    }
  }
}