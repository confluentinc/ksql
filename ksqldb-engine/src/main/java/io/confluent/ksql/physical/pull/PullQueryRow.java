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

package io.confluent.ksql.physical.pull;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;
import java.util.Optional;

public class PullQueryRow {

  private final ImmutableList<?> row;
  private final LogicalSchema schema;
  private final Optional<KsqlNode> sourceNode;

  public PullQueryRow(
      final List<?> row,
      final LogicalSchema schema,
      final Optional<KsqlNode> sourceNode) {
    this.row = ImmutableList.copyOf(row);
    this.schema = schema;
    this.sourceNode = sourceNode;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "row is ImmutableList")
  public List<?> getRow() {
    return row;
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public Optional<KsqlNode> getSourceNode() {
    return sourceNode;
  }

  public GenericRow getGenericRow() {
    return toGenericRow(row);
  }

  private static GenericRow toGenericRow(final List<?> values) {
    return new GenericRow().appendAll(values);
  }
}
