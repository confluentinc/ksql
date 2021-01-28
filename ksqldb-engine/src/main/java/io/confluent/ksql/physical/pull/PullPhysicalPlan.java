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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlKey;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.physical.pull.operators.AbstractPhysicalOperator;
import io.confluent.ksql.physical.pull.operators.DataSourceOperator;
import io.confluent.ksql.planner.plan.KeyConstraint;
import io.confluent.ksql.planner.plan.KeyConstraint.ConstraintOperator;
import io.confluent.ksql.planner.plan.LookupConstraint;
import io.confluent.ksql.query.PullQueryQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the physical plan for pull queries. It is a tree of physical operators that gets
 * created from the translation of the logical plan.
 * The root operator is always a ProjectOperator whereas the leaves are scan operators that scan
 * the data stores.
 */
public class PullPhysicalPlan {
  private static final Logger LOGGER = LoggerFactory.getLogger(PullPhysicalPlan.class);

  private final AbstractPhysicalOperator root;
  private final LogicalSchema schema;
  private final QueryId queryId;
  private final List<LookupConstraint> lookupConstraints;
  private final Materialization mat;
  private final DataSourceOperator dataSourceOperator;

  public PullPhysicalPlan(
      final AbstractPhysicalOperator root,
      final LogicalSchema schema,
      final QueryId queryId,
      final List<LookupConstraint> lookupConstraints,
      final Materialization mat,
      final DataSourceOperator dataSourceOperator
  ) {
    this.root = Objects.requireNonNull(root, "root");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.lookupConstraints = Objects.requireNonNull(lookupConstraints, "lookupConstraints");
    this.mat = Objects.requireNonNull(mat, "mat");
    this.dataSourceOperator = Objects.requireNonNull(
        dataSourceOperator, "dataSourceOperator");
  }

  public void execute(
      final List<KsqlPartitionLocation> locations,
      final PullQueryQueue pullQueryQueue,
      final BiFunction<List<?>, LogicalSchema, PullQueryRow> rowFactory) {

    // We only know at runtime which partitions to get from which node.
    // That's why we need to set this explicitly for the dataSource operators
    dataSourceOperator.setPartitionLocations(locations);

    open();
    List<?> row;
    while ((row = (List<?>)next()) != null) {
      if (pullQueryQueue.isClosed()) {
        // If the queue has been closed, we stop adding rows and cleanup. This should be triggered
        // because the client has closed their connection with the server before the results have
        // completed.
        LOGGER.info("Queue closed before results completed. Stopping execution.");
        break;
      }
      if (!pullQueryQueue.acceptRow(rowFactory.apply(row, schema))) {
        LOGGER.info("Failed to queue row");
      }
    }
    close();
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

  public List<KsqlKey> getKeys() {
    if (requiresRequestsToAllPartitions()) {
      return Collections.emptyList();
    }
    return lookupConstraints.stream()
        .filter(lookupConstraint -> lookupConstraint instanceof KeyConstraint)
        .map(KeyConstraint.class::cast)
        .filter(keyConstraint -> keyConstraint.getConstraintOperator() == ConstraintOperator.EQUAL)
        .map(KeyConstraint::getKsqlKey)
        .collect(ImmutableList.toImmutableList());
  }

  private boolean requiresRequestsToAllPartitions() {
    return lookupConstraints.stream()
        .anyMatch(lookupConstraint -> {
          if (lookupConstraint instanceof KeyConstraint) {
            final KeyConstraint keyConstraint = (KeyConstraint) lookupConstraint;
            return keyConstraint.getConstraintOperator() != ConstraintOperator.EQUAL;
          }
          return true;
        });
  }

  public LogicalSchema getOutputSchema() {
    return schema;
  }

  public QueryId getQueryId() {
    return queryId;
  }
}
