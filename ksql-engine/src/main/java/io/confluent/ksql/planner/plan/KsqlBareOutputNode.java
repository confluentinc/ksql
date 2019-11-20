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

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class KsqlBareOutputNode extends OutputNode {

  private final KeyField keyField;

  public KsqlBareOutputNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final OptionalInt limit,
      final Optional<TimestampColumn> timestampColumn
  ) {
    super(id, source, schema, limit, timestampColumn);
    this.keyField = KeyField.of(source.getKeyField().ref())
        .validateKeyExistsIn(schema);
  }

  @Override
  public QueryId getQueryId(final QueryIdGenerator queryIdGenerator) {
    return new QueryId(String.valueOf(Math.abs(ThreadLocalRandom.current().nextLong())));
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    return getSource().buildStream(builder);
  }
}
