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
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.schema.ksql.LogicalColumn;
import java.util.Objects;

public class SelectNode extends SingleInputNode<SelectNode> {
  final ImmutableList<LogicalColumn> outputSchema;

  public SelectNode(
      final Node<?> input,
      final Select selectClause
  ) {
    super(input);
    Objects.requireNonNull(selectClause, "selectClause");

    // expression after check only works with '*''
    if (selectClause.getSelectItems().size() > 1
        || !(selectClause.getSelectItems().get(0) instanceof AllColumns)) {
      throw new UnsupportedOperationException("Only `SELECT *` supported");
    }
    outputSchema = selectClause.getSelectItems().stream().flatMap(
        s -> input.getOutputSchema().stream()
    ).collect(ImmutableList.toImmutableList());

  }

  public ImmutableList<LogicalColumn> getOutputSchema() {
    return outputSchema;
  }

  public Node<?> getInputNode() {
    return input;
  }

  public <ReturnsT> ReturnsT accept(final NodeVisiter<SelectNode, ReturnsT> visitor) {
    return visitor.process(this);
  }
}
