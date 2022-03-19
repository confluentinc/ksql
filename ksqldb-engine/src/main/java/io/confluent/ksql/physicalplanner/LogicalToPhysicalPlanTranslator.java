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

package io.confluent.ksql.physicalplanner;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.plan.StreamSelect;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.logicalplanner.nodes.Node;
import io.confluent.ksql.logicalplanner.nodes.NodeVisiter;
import io.confluent.ksql.logicalplanner.nodes.SelectNode;
import io.confluent.ksql.logicalplanner.nodes.StreamSourceNode;
import io.confluent.ksql.logicalplanner.nodes.TableSourceNode;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalColumn;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;
import java.util.Optional;

public class LogicalToPhysicalPlanTranslator implements NodeVisiter<Node<?>, ExecutionStep<?>> {
  private final MetaStore metaStore;

  LogicalToPhysicalPlanTranslator(final MetaStore metaStore) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
  }

  @Override
  public ExecutionStep<?> process(final Node<?> node) {
    if (node instanceof StreamSourceNode) {
      return processStreamSourceNode((StreamSourceNode) node);
    } else if (node instanceof TableSourceNode) {
      return processTableSourceNode((TableSourceNode) node);
    } else if (node instanceof SelectNode) {
      return processSelectNode((SelectNode) node);
    } else {
      throw new IllegalStateException("Unknown node type: " + node.getClass());
    }
  }

  private StreamSource processStreamSourceNode(final StreamSourceNode streamSourceNode) {
    final KsqlTopic sourceTopic =
        metaStore.getSource(streamSourceNode.getSourceName()).getKsqlTopic();

    return ExecutionStepFactory.streamSource(
        new Stacker().push("SOURCE"),
        streamSourceNode.getSimpleSchema(),
        "test_topic",
        Formats.from(sourceTopic),
        Optional.empty(),
        1 // to-do
    );

  }

  private ExecutionStep<?> processTableSourceNode(final TableSourceNode tableSourceNode) {
    throw new UnsupportedOperationException("not implemented yet");
  }

  private StreamSelect processSelectNode(final SelectNode selectNode) {
    final StreamSource inputStep = (StreamSource) process(selectNode.getInputNode());
    final LogicalSchema inputSchema = inputStep.getSourceSchema();

    final ImmutableList<ColumnName> selectedColumns = selectNode.getOutputSchema().stream()
        .map(LogicalColumn::name)
        .collect(ImmutableList.toImmutableList());

    return ExecutionStepFactory.streamSelect(
        new Stacker().push("SELECT"),
        inputStep,
        inputSchema.key().stream().map(Column::name).collect(ImmutableList.toImmutableList()),
        inputSchema.value().stream()
            .map(Column::name)
            .filter(selectedColumns::contains)
            .map(
              columnName -> SelectExpression.of(
                  columnName,
                new UnqualifiedColumnReferenceExp(columnName)
              )
            ).collect(ImmutableList.toImmutableList())
    );
  }
}
