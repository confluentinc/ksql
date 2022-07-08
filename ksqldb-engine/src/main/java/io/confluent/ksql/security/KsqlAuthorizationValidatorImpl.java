/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.security;

import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.topic.SourceTopicsExtractor;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Set;
import org.apache.kafka.common.acl.AclOperation;

/**
 * This authorization implementation checks if the user can perform Kafka and/or SR operations
 * on the topics or schemas found in the specified KSQL {@link Statement}.
 * </p>
 * This validator only works on Kakfa 2.3 or later.
 */
public class KsqlAuthorizationValidatorImpl implements KsqlAuthorizationValidator {
  private final KsqlAccessValidator accessValidator;

  public KsqlAuthorizationValidatorImpl(final KsqlAccessValidator accessValidator) {
    this.accessValidator = accessValidator;
  }

  KsqlAccessValidator getAccessValidator() {
    return accessValidator;
  }

  @Override
  public void checkAuthorization(
      final KsqlSecurityContext securityContext,
      final MetaStore metaStore,
      final Statement statement
  ) {
    if (statement instanceof Query) {
      validateQuery(securityContext, metaStore, (Query)statement);
    } else if (statement instanceof InsertInto) {
      validateInsertInto(securityContext, metaStore, (InsertInto)statement);
    } else if (statement instanceof CreateAsSelect) {
      validateCreateAsSelect(securityContext, metaStore, (CreateAsSelect)statement);
    } else if (statement instanceof PrintTopic) {
      validatePrintTopic(securityContext, (PrintTopic)statement);
    } else if (statement instanceof CreateSource) {
      validateCreateSource(securityContext, (CreateSource)statement);
    }
  }

  private void validateQuery(
      final KsqlSecurityContext securityContext,
      final MetaStore metaStore,
      final Query query
  ) {
    for (KsqlTopic ksqlTopic : extractQueryTopics(query, metaStore)) {
      checkTopicAccess(securityContext, ksqlTopic.getKafkaTopicName(), AclOperation.READ);
      checkSchemaAccess(securityContext, ksqlTopic, AclOperation.READ);
    }
  }

  private void validateCreateAsSelect(
      final KsqlSecurityContext securityContext,
      final MetaStore metaStore,
      final CreateAsSelect createAsSelect
  ) {
    /*
     * Check topic access for CREATE STREAM/TABLE AS SELECT statements.
     *
     * Validates Write on the target topic if exists, and Read on the query sources topics.
     *
     * The Create access is validated by the TopicCreateInjector which will attempt to create
     * the target topic using the same ServiceContext used for validation.
     */

    validateQuery(securityContext, metaStore, createAsSelect.getQuery());

    // At this point, the topic should have been created by the TopicCreateInjector
    final String kafkaTopic = getCreateAsSelectSinkTopic(metaStore, createAsSelect);
    checkTopicAccess(securityContext, kafkaTopic, AclOperation.WRITE);
  }

  private void validateInsertInto(
      final KsqlSecurityContext securityContext,
      final MetaStore metaStore,
      final InsertInto insertInto
  ) {
    /*
     * Check topic access for INSERT INTO statements.
     *
     * Validates Write on the target topic, and Read on the query sources topics.
     */

    validateQuery(securityContext, metaStore, insertInto.getQuery());

    final String kafkaTopic = getSourceTopicName(metaStore, insertInto.getTarget());
    checkTopicAccess(securityContext, kafkaTopic, AclOperation.WRITE);
  }

  private void validatePrintTopic(
          final KsqlSecurityContext securityContext,
          final PrintTopic printTopic
  ) {
    checkTopicAccess(securityContext, printTopic.getTopic(), AclOperation.READ);

    // SchemaRegistry permissions cannot be validated here because the schema is guessed when
    // printing the topic by obtaining the first row and attempt to find the right schema
  }

  private void validateCreateSource(
      final KsqlSecurityContext securityContext,
      final CreateSource createSource
  ) {
    final String sourceTopic = createSource.getProperties().getKafkaTopic();
    checkTopicAccess(securityContext, sourceTopic, AclOperation.READ);

    // SchemaRegistry permissions are validated when SchemaRegisterInjector is called during CREATE
    // operations. There's no need to validate the user has READ permissions here.
  }

  private String getSourceTopicName(final MetaStore metaStore, final SourceName streamOrTable) {
    final DataSource dataSource = metaStore.getSource(streamOrTable);
    if (dataSource == null) {
      throw new KsqlException("Cannot validate for topic access from an unknown stream/table: "
          + streamOrTable);
    }

    return dataSource.getKafkaTopicName();
  }

  private String getCreateAsSelectSinkTopic(
      final MetaStore metaStore,
      final CreateAsSelect createAsSelect
  ) {
    return createAsSelect.getProperties().getKafkaTopic()
        .orElseGet(() -> getSourceTopicName(metaStore, createAsSelect.getName()));
  }

  private Set<KsqlTopic> extractQueryTopics(final Query query, final MetaStore metaStore) {
    final SourceTopicsExtractor extractor = new SourceTopicsExtractor(metaStore);
    extractor.process(query, null);
    return extractor.getSourceTopics();
  }

  private void checkTopicAccess(
      final KsqlSecurityContext securityContext,
      final String resourceName,
      final AclOperation operation
  ) {
    accessValidator.checkTopicAccess(securityContext, resourceName, operation);
  }

  private void checkSchemaAccess(
      final KsqlSecurityContext securityContext,
      final KsqlTopic ksqlTopic,
      final AclOperation operation
  ) {

    if (formatSupportsSchemaInference(ksqlTopic.getKeyFormat().getFormatInfo())) {
      accessValidator.checkSubjectAccess(securityContext,
          KsqlConstants.getSRSubject(ksqlTopic.getKafkaTopicName(), true), operation);
    }

    if (formatSupportsSchemaInference(ksqlTopic.getValueFormat().getFormatInfo())) {
      accessValidator.checkSubjectAccess(securityContext,
          KsqlConstants.getSRSubject(ksqlTopic.getKafkaTopicName(), false), operation);
    }
  }

  private static boolean formatSupportsSchemaInference(final FormatInfo format) {
    return FormatFactory.of(format).supportsFeature(SerdeFeature.SCHEMA_INFERENCE);
  }
}
