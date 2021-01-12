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

import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.Optional;
import java.util.OptionalInt;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class KsqlBareOutputNode extends OutputNode {

  private final Optional<WindowInfo> windowInfo;

  public KsqlBareOutputNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final OptionalInt limit,
      final Optional<TimestampColumn> timestampColumn,
      final Optional<WindowInfo> windowInfo
  ) {
    super(id, source, schema, limit, timestampColumn);
    this.windowInfo = requireNonNull(windowInfo, "windowInfo");
  }

  @Override
  public Optional<SourceName> getSinkName() {
    return Optional.empty();
  }

  @Override
  public SchemaKStream<?> buildStream(final PlanBuildContext buildContext) {
    return getSource().buildStream(buildContext);
  }

  public Optional<WindowInfo> getWindowInfo() {
    return windowInfo;
  }
}
