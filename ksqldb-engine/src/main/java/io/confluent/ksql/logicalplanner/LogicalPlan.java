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

package io.confluent.ksql.logicalplanner;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.logicalplanner.nodes.Node;
import io.confluent.ksql.name.SourceName;
import java.util.Objects;

/**
 * A logical plan represent a query as a graph of logical {@link Node}s like
 * input stream/tables (as leaf nodes), projection, filters, grouping/aggregations, join, etc,
 * and a single (root node).
 *
 * <p>Note that the root is not a "sink", because the logical plan does not know anything about
 * physical properties (writing the result into a "sink topic" is a physical operation).
 */
public class LogicalPlan {
  private final Node<?> root;
  private final ImmutableSet<SourceName> sourceNames;

  LogicalPlan(final Node<?> root, final ImmutableSet<SourceName> sourceNames) {
    this.root = Objects.requireNonNull(root, "root");
    this.sourceNames = Objects.requireNonNull(sourceNames, "sourceNames");
  }

  public Node<?> getRoot() {
    return root;
  }

  public ImmutableSet<SourceName> getSourceNames() {
    return sourceNames;
  }
}
