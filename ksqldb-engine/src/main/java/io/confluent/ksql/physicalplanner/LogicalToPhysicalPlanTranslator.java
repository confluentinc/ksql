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
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.logicalplanner.nodes.Node;
import io.confluent.ksql.logicalplanner.nodes.NodeVisitor;
import io.confluent.ksql.logicalplanner.nodes.SelectNode;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalColumn;
import java.util.Objects;

public class LogicalToPhysicalPlanTranslator
    implements NodeVisitor<Node<?>, io.confluent.ksql.physicalplanner.nodes.Node<?>> {
  private final MetaStore metaStore;

  LogicalToPhysicalPlanTranslator(final MetaStore metaStore) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
  }

  @Override
  public io.confluent.ksql.physicalplanner.nodes.Node<?> process(
      final io.confluent.ksql.logicalplanner.nodes.Node<?> node
  ) {
    if (node instanceof io.confluent.ksql.logicalplanner.nodes.StreamSourceNode) {
      return processStreamSourceNode(
          (io.confluent.ksql.logicalplanner.nodes.StreamSourceNode) node
      );
//    } else if (node instanceof TableSourceNode) {
//      final TableSourceNode tableSourceNode = (TableSourceNode) node;
//      final io.confluent.ksql.physicalplanner.nodes.Node<?> input =
//          process(tableSourceNode.getInputNode());
//      return processTableSourceNode(input, tableSourceNode);
    } else if (node instanceof SelectNode) {
      final SelectNode selectNode = (SelectNode) node;
      final io.confluent.ksql.physicalplanner.nodes.Node<?> input =
          process(selectNode.getInputNode());
      return processSelectNode(input, selectNode);
    } else {
      throw new IllegalStateException("Unknown node type: " + node.getClass());
    }
  }

  private io.confluent.ksql.physicalplanner.nodes.StreamSourceNode processStreamSourceNode(
      final io.confluent.ksql.logicalplanner.nodes.StreamSourceNode node
  ) {
    final SourceName sourceName = node.getSourceName();
    final KsqlTopic topic = metaStore.getSource(sourceName).getKsqlTopic();

    return new io.confluent.ksql.physicalplanner.nodes.StreamSourceNode(
        sourceName,
        node.getSimpleSchema(),
        topic.getKeyFormat(),
        topic.getValueFormat()
    );
  }

  private io.confluent.ksql.physicalplanner.nodes.Node<?> processSelectNode(
      final io.confluent.ksql.physicalplanner.nodes.Node<?> input,
      final SelectNode selectNode
  ) {
    final ImmutableList<ColumnName> selectedColumns = selectNode.getOutputSchema().stream()
        .map(LogicalColumn::name)
        .collect(ImmutableList.toImmutableList());

    final ImmutableList<ColumnName> selectedKeys = input.keyColumnNames().stream()
        .filter(selectedColumns::contains)
        .collect(ImmutableList.toImmutableList());

    final ImmutableList<ColumnName> selectedValue = input.valueColumnNames().stream()
        .filter(selectedColumns::contains)
        .collect(ImmutableList.toImmutableList());

    return new io.confluent.ksql.physicalplanner.nodes.SelectNode(
        input,
        selectedKeys,
        selectedValue
    );
  }

}
