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

package io.confluent.ksql.topic;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.TopicDescription;

public class DefaultTopicInjector implements TopicInjector {

  private final KafkaTopicClient topicClient;
  private final MetaStore metaStore;

  public DefaultTopicInjector(
      final KsqlExecutionContext executionContext
  ) {
    this(executionContext.getServiceContext().getTopicClient(), executionContext.getMetaStore());
  }

  DefaultTopicInjector(
      final KafkaTopicClient topicClient,
      final MetaStore metaStore) {
    this.topicClient = Objects.requireNonNull(topicClient, "topicClient");
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Statement> PreparedStatement<T> forStatement(
      final PreparedStatement<T> statement,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> propertyOverrides) {
    if (!(statement.getStatement() instanceof CreateAsSelect)) {
      return statement;
    }

    final PreparedStatement<? extends CreateAsSelect> cas =
        (PreparedStatement<? extends CreateAsSelect>) statement;

    final String topic = topicName(cas.getStatement(), ksqlConfig);
    final TopicDescription description = describeSource(topicClient, ksqlConfig, cas);
    final int partitions = numPartitions(cas, ksqlConfig, propertyOverrides, description);
    final short replicas = numReplicas(cas, ksqlConfig, propertyOverrides, description);

    final Map<String, Expression> properties = new HashMap<>(cas.getStatement().getProperties());
    properties.put(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(topic));
    properties.put(KsqlConstants.SINK_NUMBER_OF_REPLICAS, new IntegerLiteral(replicas));
    properties.put(KsqlConstants.SINK_NUMBER_OF_PARTITIONS, new IntegerLiteral(partitions));

    final CreateAsSelect withTopic = cas.getStatement().copyWith(properties);
    final String withTopicText = SqlFormatter.formatSql(withTopic) + ";";

    return (PreparedStatement<T>) PreparedStatement.of(withTopicText, withTopic);
  }

  private TopicDescription describeSource(
      final KafkaTopicClient topicClient,
      final KsqlConfig ksqlConfig,
      final PreparedStatement<? extends CreateAsSelect> cas
  ) {
    final Analysis analysis = new QueryAnalyzer(
        metaStore,
        ksqlConfig.getString(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG))
        .analyze(
            cas.getStatementText(),
            cas.getStatement().getQuery(),
            Optional.of(cas.getStatement().getSink()));

    final StructuredDataSource theSource = analysis.getTheSource();
    final String kafkaTopicName = theSource.getKsqlTopic().getKafkaTopicName();
    return topicClient.describeTopic(kafkaTopicName);
  }

  private static String topicName(
      final CreateAsSelect cas,
      final KsqlConfig cfg
  ) {
    final Expression topicProperty = cas.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY);
    return (topicProperty == null)
        ? cfg.getString(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG) + cas.getName().getSuffix()
        : StringUtils.strip(topicProperty.toString(), "'");
  }

  private static int numPartitions(
      final PreparedStatement<? extends CreateAsSelect> cas,
      final KsqlConfig cfg,
      final Map<String, Object> propertyOverrides,
      final TopicDescription description
  ) {
    final Optional<Integer> inProperties = checkedParse(
        Integer::parseInt, cas, KsqlConstants.SINK_NUMBER_OF_PARTITIONS);
    if (inProperties.isPresent()) {
      return inProperties.get();
    }

    final Integer legacyNumPartitions = cfg.getInt(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY);
    return checkedParse(
          Integer::parseInt,
          KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY,
          propertyOverrides)
        .orElseGet(() -> ObjectUtils.defaultIfNull(
            legacyNumPartitions,
            description.partitions().size()));
  }

  private static short numReplicas(
      final PreparedStatement<? extends CreateAsSelect> cas,
      final KsqlConfig cfg,
      final Map<String, Object> propertyOverrides,
      final TopicDescription description
  ) {
    final Optional<Short> inProperties = checkedParse(
        Short::parseShort, cas, KsqlConstants.SINK_NUMBER_OF_REPLICAS);
    if (inProperties.isPresent()) {
      return inProperties.get();
    }

    final Short legacyNumReplicas = cfg.getShort(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY);
    return checkedParse(
          Short::parseShort,
          KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY,
          propertyOverrides)
        .orElseGet(() -> ObjectUtils.defaultIfNull(
            legacyNumReplicas,
            (short) description.partitions().get(0).replicas().size()));
  }

  private static <T extends Number> Optional<T> checkedParse(
      final Function<String, T> parse,
      final PreparedStatement<? extends CreateAsSelect> cas,
      final String property
  ) {
    final Object exp = cas.getStatement().getProperties().get(property);
    if (exp == null) {
      return Optional.empty();
    }

    final String val = exp.toString();
    try {
      return Optional.ofNullable(parse.apply(val));
    } catch (NumberFormatException e) {
      throw new KsqlStatementException(
          String.format("Invalid value for property %s: %s",
              property, val),
          cas.getStatementText());
    }
  }

  private static <T extends Number> Optional<T> checkedParse(
      final Function<String, T> parse,
      final String property,
      final Map<String, ?> propertyOverrides) {
    final Object override = propertyOverrides.get(property);
    if (override == null) {
      return Optional.empty();
    }

    final String val = override.toString();
    try {
      return Optional.ofNullable(parse.apply(val));
    } catch (NumberFormatException e) {
      throw new KsqlException(
          String.format("Invalid property override %s: %s (all overrides: %s)",
              property, val, propertyOverrides));
    }
  }
}
