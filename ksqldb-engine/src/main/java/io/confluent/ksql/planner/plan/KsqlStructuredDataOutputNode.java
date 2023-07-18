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

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KsqlStructuredDataOutputNode extends OutputNode {

  private final KsqlTopic ksqlTopic;
  private final boolean doCreateInto;
  private final SourceName sinkName;
  private final boolean orReplace;

  public KsqlStructuredDataOutputNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final Optional<TimestampColumn> timestampColumn,
      final KsqlTopic ksqlTopic,
      final OptionalInt limit,
      final boolean doCreateInto,
      final SourceName sinkName,
      final boolean orReplace
  ) {
    super(id, source, schema, limit, timestampColumn);

    this.ksqlTopic = requireNonNull(ksqlTopic, "ksqlTopic");
    this.doCreateInto = doCreateInto;
    this.sinkName = requireNonNull(sinkName, "sinkName");
    this.orReplace = orReplace;

    validate(source, sinkName);
  }

  public boolean createInto() {
    return doCreateInto;
  }

  public KsqlTopic getKsqlTopic() {
    return ksqlTopic;
  }

  public boolean getOrReplace() {
    return orReplace;
  }

  @Override
  public Optional<SourceName> getSinkName() {
    return Optional.of(sinkName);
  }

  @Override
  public SchemaKStream<?> buildStream(final PlanBuildContext buildContext) {
    final PlanNode source = getSource();
    final SchemaKStream<?> schemaKStream = source.buildStream(buildContext);

    final QueryContext.Stacker contextStacker = buildContext.buildNodeContext(getId().toString());

    return schemaKStream.into(
        ksqlTopic,
        contextStacker,
        getTimestampColumn()
    );
  }

  private static void validate(
      final PlanNode source,
      final SourceName sinkName
  ) {
    if (!(source instanceof VerifiableNode)) {
      throw new IllegalArgumentException("VerifiableNode required");
    }

    ((VerifiableNode) source)
        .validateKeyPresent(sinkName);

    final LogicalSchema schema = source.getSchema();

    final String duplicates = schema.columns().stream()
        .map(Column::name)
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
        .entrySet()
        .stream()
        .filter(e -> e.getValue() > 1)
        .map(Entry::getKey)
        .map(ColumnName::toString)
        .collect(Collectors.joining(", "));

    if (!duplicates.isEmpty()) {
      throw new IllegalArgumentException("Value columns clash with key columns: " + duplicates);
    }
  }
}
