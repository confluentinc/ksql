/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

/**
 * Metadata of a persistent query, e.g. {@code CREATE STREAM FOO AS SELECT * FROM BAR;}.
 */
public class PersistentQueryMetadata extends QueryMetadata {

  private final QueryId id;
  private final KsqlTopic resultTopic;
  private final Set<String> sinkNames;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public PersistentQueryMetadata(
      final String statementString,
      final KafkaStreams kafkaStreams,
      final Schema resultSchema,
      final Set<String> sourceNames,
      final StructuredDataSource sinkDataSource,
      final String executionPlan,
      final QueryId id,
      final DataSource.DataSourceType dataSourceType,
      final String queryApplicationId,
      final KsqlTopic resultTopic,
      final Topology topology,
      final Map<String, Object> streamsProperties,
      final Map<String, Object> overriddenProperties,
      final Consumer<QueryMetadata> closeCallback
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    super(
        statementString,
        kafkaStreams,
        resultSchema,
        sourceNames,
        executionPlan,
        dataSourceType,
        queryApplicationId,
        topology,
        streamsProperties,
        overriddenProperties,
        closeCallback);
    this.id = Objects.requireNonNull(id, "id");
    this.resultTopic = Objects.requireNonNull(resultTopic, "resultTopic");
    this.sinkNames = ImmutableSet.of(sinkDataSource.getName());

    if (resultTopic.getKsqlTopicSerDe() == null) {
      throw new KsqlException(String.format("Invalid result topic: %s. Serde cannot be null.",
          resultTopic.getName()));
    }
  }

  private PersistentQueryMetadata(
      final PersistentQueryMetadata other,
      final Consumer<QueryMetadata> closeCallback
  ) {
    super(other, closeCallback);
    this.id = other.id;
    this.resultTopic = other.resultTopic;
    this.sinkNames = other.sinkNames;
  }

  public PersistentQueryMetadata copyWith(final Consumer<QueryMetadata> closeCallback) {
    return new PersistentQueryMetadata(this, closeCallback);
  }

  public QueryId getQueryId() {
    return id;
  }

  public KsqlTopic getResultTopic() {
    return resultTopic;
  }

  public Set<String> getSinkNames() {
    return sinkNames;
  }

  public DataSource.DataSourceSerDe getResultTopicSerde() {
    return resultTopic.getKsqlTopicSerDe().getSerDe();
  }
}
