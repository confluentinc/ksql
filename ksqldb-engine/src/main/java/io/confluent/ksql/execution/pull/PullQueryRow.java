/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.pull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class PullQueryRow {

  private final List<?> row;
  private final LogicalSchema schema;
  private final Optional<KsqlHostInfoEntity> sourceNode;
  private final Optional<ConsistencyOffsetVector> consistencyOffsetVector;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public PullQueryRow(
      final List<?> row,
      final LogicalSchema schema,
      final Optional<KsqlHostInfoEntity> sourceNode,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector
  ) {
    this.row = row;
    this.schema = schema;
    this.sourceNode = sourceNode;
    this.consistencyOffsetVector = consistencyOffsetVector;
  }

  public List<?> getRow() {
    return Collections.unmodifiableList(row);
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public Optional<KsqlHostInfoEntity> getSourceNode() {
    return sourceNode;
  }

  public Optional<ConsistencyOffsetVector> getConsistencyOffsetVector() {
    return consistencyOffsetVector;
  }

  public GenericRow getGenericRow() {
    return toGenericRow(row);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PullQueryRow{");
    sb.append("row=").append(row);
    sb.append(", schema=").append(schema);
    sb.append(", sourceNode=").append(sourceNode);
    sb.append(", consistencyOffsetVector=").append(consistencyOffsetVector);
    sb.append('}');
    return sb.toString();
  }

  private static GenericRow toGenericRow(final List<?> values) {
    return new GenericRow().appendAll(values);
  }
}
