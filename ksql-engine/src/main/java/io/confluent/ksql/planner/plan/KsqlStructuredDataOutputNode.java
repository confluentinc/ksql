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

package io.confluent.ksql.planner.plan;

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.KsqlSchema;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;

public class KsqlStructuredDataOutputNode extends OutputNode {

  private final String kafkaTopicName;
  private final KsqlTopic ksqlTopic;
  private final KeyField keyField;
  private final boolean doCreateInto;
  private final boolean selectKeyRequired;

  public KsqlStructuredDataOutputNode(
      final PlanNodeId id,
      final PlanNode source,
      final KsqlSchema schema,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final KeyField keyField,
      final KsqlTopic ksqlTopic,
      final String kafkaTopicName,
      final boolean selectKeyRequired,
      final Optional<Integer> limit,
      final boolean doCreateInto
  ) {
    super(id, source, schema, limit, timestampExtractionPolicy);
    this.kafkaTopicName = Objects.requireNonNull(kafkaTopicName, "kafkaTopicName");
    this.keyField = Objects.requireNonNull(keyField, "keyField")
        .validateKeyExistsIn(schema);
    this.ksqlTopic = Objects.requireNonNull(ksqlTopic, "ksqlTopic");
    this.selectKeyRequired = selectKeyRequired;
    this.doCreateInto = doCreateInto;

    if (selectKeyRequired && !keyField.name().isPresent()) {
      throw new IllegalArgumentException("keyField must be provided when performing partition by");
    }
  }

  public String getKafkaTopicName() {
    return kafkaTopicName;
  }

  public boolean isDoCreateInto() {
    return doCreateInto;
  }

  @Override
  public QueryId getQueryId(final QueryIdGenerator queryIdGenerator) {
    final String base = queryIdGenerator.getNextId();
    if (!doCreateInto) {
      return new QueryId("InsertQuery_" + base);
    }
    if (getNodeOutputType().equals(DataSourceType.KTABLE)) {
      return new QueryId("CTAS_" + getId().toString() + "_" + base);
    }
    return new QueryId("CSAS_" + getId().toString() + "_" + base);
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    final PlanNode source = getSource();
    final SchemaKStream schemaKStream = source.buildStream(builder);

    final QueryContext.Stacker contextStacker = builder.buildNodeContext(getId());

    final Set<Integer> rowkeyIndexes = getSchema().implicitColumnIndexes();
    final KsqlSchema schema = getSchema().withoutImplicitFields();

    final SchemaKStream<?> result = createOutputStream(
        schemaKStream,
        builder.getKsqlConfig(),
        builder.getFunctionRegistry(),
        contextStacker
    );

    final Serde<GenericRow> outputRowSerde = builder.buildGenericRowSerde(
        getKsqlTopic().getValueSerdeFactory(),
        schema.getSchema(),
        contextStacker.getQueryContext());

    result.into(
        getKafkaTopicName(),
        outputRowSerde,
        rowkeyIndexes
    );

    result.setOutputNode(this);
    return result;
  }

  @SuppressWarnings("unchecked")
  private SchemaKStream<?> createOutputStream(
      final SchemaKStream schemaKStream,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final QueryContext.Stacker contextStacker
  ) {

    if (schemaKStream instanceof SchemaKTable) {
      return schemaKStream;
    }

    final KeyField resultKeyField = KeyField.of(
        schemaKStream.getKeyField().name(),
        getKeyField().legacy()
    );

    final SchemaKStream result = new SchemaKStream(
        getSchema(),
        schemaKStream.getKstream(),
        resultKeyField,
        Collections.singletonList(schemaKStream),
        schemaKStream.getKeySerdeFactory(),
        SchemaKStream.Type.SINK,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );

    if (!selectKeyRequired) {
      return result;
    }

    final Field newKey = keyField.resolveLatest(getSchema())
        .orElseThrow(IllegalStateException::new);

    return result.selectKey(newKey.name(), false, contextStacker);
  }

  public KsqlTopic getKsqlTopic() {
    return ksqlTopic;
  }
}