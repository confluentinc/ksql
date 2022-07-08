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

package io.confluent.ksql.execution;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.plan.StreamSelect;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.physicalplanner.nodes.Node;
import io.confluent.ksql.physicalplanner.nodes.NodeVisitor;
import io.confluent.ksql.physicalplanner.nodes.SelectNode;
import io.confluent.ksql.physicalplanner.nodes.StreamSourceNode;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class PhysicalToExecutionPlanTranslator implements NodeVisitor<Node<?>, ExecutionStep<?>> {
  private final MetaStore metaStore;

  PhysicalToExecutionPlanTranslator(final MetaStore metaStore) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
  }

  @Override
  public ExecutionStep<?> process(final Node<?> node) {
    if (node instanceof StreamSourceNode) {
      return processStreamSourceNode((StreamSourceNode) node);
    //} else if (node instanceof TableSourceNode) {
    //  return processTableSourceNode((TableSourceNode) node);
    } else if (node instanceof SelectNode) {
      return processSelectNode((SelectNode) node);
    } else {
      throw new IllegalStateException("Unknown node type: " + node.getClass());
    }
  }

  private StreamSource processStreamSourceNode(final StreamSourceNode streamSourceNode) {
    return ExecutionStepFactory.streamSource(
        new Stacker().push("SOURCE"),
        streamSourceNode.getSimpleSchema(),
        metaStore.getSource(streamSourceNode.getSourceName()).getKsqlTopic().getKafkaTopicName(),
        streamSourceNode.getFormats(),
        Optional.empty(),
        1 // to-do
    );

  }

  //private ExecutionStep<?> processTableSourceNode(final TableSourceNode tableSourceNode) {
  //  throw new UnsupportedOperationException("not implemented yet");
  //}

  private StreamSelect<GenericKey> processSelectNode(final SelectNode selectNode) {
    final ExecutionStep<KStreamHolder<GenericKey>> inputStep =
        (ExecutionStep<KStreamHolder<GenericKey>>) process(selectNode.getInputNode());

    final List<SelectExpression> selectExpressions = selectNode.valueColumnNames().stream()
        .map(columnName -> SelectExpression.of(
            columnName,
            new UnqualifiedColumnReferenceExp(columnName)
        ))
        .collect(ImmutableList.toImmutableList());

    return ExecutionStepFactory.streamSelect(
        new Stacker().push("SELECT"),
        inputStep,
        selectNode.keyColumnNames(),
        selectExpressions
    );
  }
}
