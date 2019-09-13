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
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

public class KsqlStructuredDataOutputNode extends OutputNode {

  private final KsqlTopic ksqlTopic;
  private final KeyField keyField;
  private final Optional<ColumnName> partitionByField;
  private final boolean doCreateInto;
  private final Set<SerdeOption> serdeOptions;
  private final SourceName intoSourceName;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public KsqlStructuredDataOutputNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final KeyField keyField,
      final KsqlTopic ksqlTopic,
      final Optional<ColumnName> partitionByField,
      final OptionalInt limit,
      final boolean doCreateInto,
      final Set<SerdeOption> serdeOptions,
      final SourceName intoSourceName) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    super(
        id,
        source,
        // KSQL internally copies the implicit and key fields into the value schema.
        // This is done by DataSourceNode
        // Hence, they must be removed again here if they are still in the sink schema.
        // This leads to strange behaviour, but changing it is a breaking change.
        schema.withoutMetaAndKeyColsInValue(),
        limit,
        timestampExtractionPolicy
    );

    this.serdeOptions = ImmutableSet.copyOf(requireNonNull(serdeOptions, "serdeOptions"));
    this.keyField = requireNonNull(keyField, "keyField")
        .validateKeyExistsIn(schema);
    this.ksqlTopic = requireNonNull(ksqlTopic, "ksqlTopic");
    this.partitionByField = Objects.requireNonNull(partitionByField, "partitionByField");
    this.doCreateInto = doCreateInto;
    this.intoSourceName = requireNonNull(intoSourceName, "intoSourceName");

    validatePartitionByField();
  }

  public boolean isDoCreateInto() {
    return doCreateInto;
  }

  public KsqlTopic getKsqlTopic() {
    return ksqlTopic;
  }

  public Set<SerdeOption> getSerdeOptions() {
    return serdeOptions;
  }

  public SourceName getIntoSourceName() {
    return intoSourceName;
  }

  @Override
  public QueryId getQueryId(final QueryIdGenerator queryIdGenerator, final long offset) {
    final String base = offset == -1 ? queryIdGenerator.getNextId() : String.valueOf(offset);
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

    final QueryContext.Stacker contextStacker = builder.buildNodeContext(getId().toString());

    final SchemaKStream<?> result = createOutputStream(
        schemaKStream,
        contextStacker
    );

    return result.into(
        getKsqlTopic().getKafkaTopicName(),
        getSchema(),
        getKsqlTopic().getValueFormat(),
        serdeOptions,
        contextStacker,
        builder
    );
  }

  @SuppressWarnings("unchecked")
  private SchemaKStream<?> createOutputStream(
      final SchemaKStream schemaKStream,
      final QueryContext.Stacker contextStacker
  ) {

    if (schemaKStream instanceof SchemaKTable) {
      return schemaKStream;
    }

    final KeyField resultKeyField = KeyField.of(
        schemaKStream.getKeyField().name(),
        getKeyField().legacy()
    );

    final SchemaKStream result = schemaKStream.withKeyField(resultKeyField);

    if (!partitionByField.isPresent()) {
      return result;
    }

    return result.selectKey(partitionByField.get(), false, contextStacker);
  }

  private void validatePartitionByField() {
    if (!partitionByField.isPresent()) {
      return;
    }

    final ColumnName fieldName = partitionByField.get();

    if (getSchema().isMetaColumn(fieldName) || getSchema().isKeyColumn(fieldName)) {
      return;
    }

    if (!keyField.name().equals(Optional.of(fieldName))) {
      throw new IllegalArgumentException("keyField must match partition by field");
    }
  }
}