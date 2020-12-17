/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.physical.pull;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.physical.pull.operators.AbstractPhysicalOperator;
import io.confluent.ksql.physical.pull.operators.DataSourceOperator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents the physical plan for pull queries. It is a tree of physical operators that gets
 * created from the translation of the logical plan.
 * The root operator is always a ProjectOperator whereas the leaves are scan operators that scan
 * the data stores.
 */
public class PullPhysicalPlan {
  private final AbstractPhysicalOperator root;
  private final LogicalSchema schema;
  private final QueryId queryId;
  private final List<GenericKey> keys;
  private final Materialization mat;
  private final DataSourceOperator dataSourceOperator;

  public PullPhysicalPlan(
      final AbstractPhysicalOperator root,
      final LogicalSchema schema,
      final QueryId queryId,
      final List<GenericKey> keys,
      final Materialization mat,
      final DataSourceOperator dataSourceOperator
  ) {
    this.root = Objects.requireNonNull(root, "root");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.keys = Objects.requireNonNull(keys, "keys");
    this.mat = Objects.requireNonNull(mat, "mat");
    this.dataSourceOperator = Objects.requireNonNull(
        dataSourceOperator, "dataSourceOperator");
  }

  public List<List<?>> execute(
      final List<KsqlPartitionLocation> locations) {

    // We only know at runtime which partitions to get from which node.
    // That's why we need to set this explicitly for the dataSource operators
    dataSourceOperator.setPartitionLocations(locations);

    open();
    final List<List<?>> localResult = new ArrayList<>();
    List<?> row = null;
    while ((row = (List<?>)next()) != null) {
      if (!row.isEmpty()) {
        localResult.add(row);
      }
    }
    close();

    return localResult;
  }

  private void open() {
    root.open();
  }

  private Object next() {
    return root.next();
  }

  private void close() {
    root.close();
  }

  public AbstractPhysicalOperator getRoot() {
    return root;
  }

  public Materialization getMaterialization() {
    return mat;
  }

  public List<GenericKey> getKeys() {
    return keys;
  }

  public LogicalSchema getOutputSchema() {
    return schema;
  }

  public QueryId getQueryId() {
    return queryId;
  }
}
