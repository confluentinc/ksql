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

import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;
import java.util.stream.Collectors;

public abstract class ProjectNode extends SingleSourcePlanNode {

  public ProjectNode(
      final PlanNodeId id,
      final PlanNode source
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName(), source);
  }

  public abstract List<SelectExpression> getSelectExpressions();

  @Override
  public SchemaKStream<?> buildStream(final PlanBuildContext buildContext) {
    final SchemaKStream<?> stream = getSource().buildStream(buildContext);

    final List<ColumnName> keyColumnNames = getSchema().key().stream()
        .map(Column::name)
        .collect(Collectors.toList());

    return stream.select(
        keyColumnNames,
        getSelectExpressions(),
        buildContext.buildNodeContext(getId().toString()),
        buildContext
    );
  }
}
