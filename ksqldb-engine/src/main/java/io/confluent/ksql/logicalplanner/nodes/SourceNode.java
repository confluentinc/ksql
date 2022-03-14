/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.logicalplanner.nodes;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalColumn;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;

public abstract class SourceNode<SourceT extends SourceNode<?>> implements Node<SourceT> {
  final SourceName sourceName;
  final LogicalSchema simpleSchema; // schema without system columns
  final ImmutableList<LogicalColumn> outputSchema; // to-do: should we include system columns?

  SourceNode(
      final SourceName sourceName,
      final LogicalSchema simpleSchema
  ) {
    this.sourceName = Objects.requireNonNull(sourceName, "sourceName");
    this.simpleSchema = Objects.requireNonNull(simpleSchema, "simpleSchema");

    outputSchema = simpleSchema.columns().stream()
        .map(column -> new LogicalColumn(column.name(), column.type()))
        .collect(ImmutableList.toImmutableList());
  }

  public ImmutableList<LogicalColumn> getOutputSchema() {
    return outputSchema;
  }

  public SourceName getSourceName() {
    return sourceName;
  }

  /**
   * @return the schema without system columns
   */
  public LogicalSchema getSimpleSchema() {
    return simpleSchema;
  }

}