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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.SerdeOptionsFactory;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlSchemaRegistryNotConfiguredException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.IOException;
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

    final LogicalSchema schema = cs.getStatement().getElements().toLogicalSchema();

    final SerdeOptions serdeOptions = SerdeOptionsFactory.buildForCreateStatement(
        schema,
        cs.getStatement().getProperties().getValueFormat(),
        cs.getStatement().getProperties().getSerdeOptions(),
        cs.getConfig()
    );

    registerSchema(
        schema,
        cs.getStatement().getProperties().getKafkaTopic(),
        cs.getStatement().getProperties().getFormatInfo(),
        serdeOptions,
        cs.getConfig(),
        cs.getStatementText(),
        false
    );
  }

  private void registerForCreateAs(final ConfiguredStatement<? extends CreateAsSelect> cas) {
    final ServiceContext sandboxServiceContext = SandboxedServiceContext.create(serviceContext);
    final ExecuteResult executeResult = executionContext
        .createSandbox(sandboxServiceContext)
        .execute(sandboxServiceContext, cas);

    final PersistentQueryMetadata queryMetadata = (PersistentQueryMetadata) executeResult
        .getQuery()
        .orElseThrow(() -> new KsqlStatementException(
            "Could not determine output schema for query due to error: "
                + executeResult.getCommandResult(),
            cas.getStatementText()
        ));

    registerSchema(
        queryMetadata.getLogicalSchema(),
        queryMetadata.getResultTopic().getKafkaTopicName(),
        queryMetadata.getResultTopic().getValueFormat().getFormatInfo(),
        queryMetadata.getPhysicalSchema().serdeOptions(),
        cas.getConfig(),
        cas.getStatementText(),
        true
    );
  }

  private void registerSchema(
      final LogicalSchema schema,
      final String topic,
      final FormatInfo formatInfo,
      final SerdeOptions serdeOptions,
      final KsqlConfig config,
      final String statementText,
      final boolean registerIfSchemaExists
  ) {
    final Format format = FormatFactory.of(formatInfo);
    if (!format.supportsSchemaInference()) {
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
      final String subject = topic + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

      if (registerIfSchemaExists || !srClient.getAllSubjects().contains(subject)) {
        final ParsedSchema parsedSchema = format.toParsedSchema(
            schema.withoutPseudoAndKeyColsInValue().value(),
            serdeOptions,
            formatInfo
        );

        srClient.register(subject, parsedSchema);
      }
    } catch (IOException | RestClientException e) {
      throw new KsqlStatementException("Could not register schema for topic.", statementText, e);
    }
  }
}
