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

package io.confluent.ksql.physicalplanner.nodes;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import java.util.List;
import java.util.Objects;

public final class StreamSourceNode implements Node<StreamSourceNode> {
  private final SourceName sourceName;
  private final LogicalSchema simpleSchema;
  private final KeyFormat keyFormat;
  private final ValueFormat valueFormat;
  private final ImmutableList<ColumnName> keyColumns;
  private final ImmutableList<ColumnName> valueColumns;

  public StreamSourceNode(
      final SourceName sourceName,
      final LogicalSchema simpleSchema,
      final KeyFormat keyFormat,
      final ValueFormat valueFormat
  ) {
    this.sourceName = Objects.requireNonNull(sourceName, "sourceName");
    this.simpleSchema = Objects.requireNonNull(simpleSchema, "simpleSchema");
    this.keyFormat = Objects.requireNonNull(keyFormat, "keyFormat");
    this.valueFormat = Objects.requireNonNull(valueFormat, "valueFormat");

    keyColumns = toColumNames(simpleSchema.key());
    valueColumns = toColumNames(simpleSchema.value());
  }

  private static ImmutableList<ColumnName> toColumNames(final List<Column> columns) {
    return columns.stream()
        .map(Column::name)
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public ImmutableList<ColumnName> keyColumnNames() {
    return keyColumns;
  }

  @Override
  public ImmutableList<ColumnName> valueColumnNames() {
    return valueColumns;
  }

  @Override
  public Formats getFormats() {
    return Formats.of(
        keyFormat.getFormatInfo(),
        valueFormat.getFormatInfo(),
        keyFormat.getFeatures(),
        valueFormat.getFeatures()
    );
  }

  @Override
  public <ReturnsT> ReturnsT accept(final NodeVisitor<StreamSourceNode, ReturnsT> visitor) {
    return visitor.process(this);
  }

  public SourceName getSourceName() {
    return sourceName;
  }

  public LogicalSchema getSimpleSchema() {
    return simpleSchema;
  }

}
