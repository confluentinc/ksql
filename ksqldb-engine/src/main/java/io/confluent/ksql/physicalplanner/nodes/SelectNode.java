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

public final class SelectNode extends SingleInputNode<SelectNode> {

  public SelectNode(
      final Node<?> input,
      final ImmutableList<ColumnName> selectedKeys,
      final ImmutableList<ColumnName> selectedValues
  ) {
    super(input, selectedKeys, selectedValues);
  }

  @Override
  public Formats getFormats() {
    return input.getFormats();
  }

  @Override
  public <ReturnsT> ReturnsT accept(final NodeVisitor<SelectNode, ReturnsT> visitor) {
    return visitor.process(this);
  }

}
