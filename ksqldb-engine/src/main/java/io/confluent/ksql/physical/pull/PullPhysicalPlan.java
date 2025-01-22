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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlKey;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.physical.common.QueryRow;
import io.confluent.ksql.physical.common.operators.AbstractPhysicalOperator;
import io.confluent.ksql.physical.pull.operators.DataSourceOperator;
import io.confluent.ksql.planner.plan.KeyConstraint;
import io.confluent.ksql.planner.plan.LookupConstraint;
import io.confluent.ksql.query.PullQueryWriteStream;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConstants.QuerySourceType;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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
  private final PullPhysicalPlanType pullPhysicalPlanType;
  private final QuerySourceType querySourceType;
  private final Materialization mat;
  private final DataSourceOperator dataSourceOperator;

  public PullPhysicalPlan(
      final AbstractPhysicalOperator root,
      final LogicalSchema schema,
      final QueryId queryId,
      final List<LookupConstraint> lookupConstraints,
      final PullPhysicalPlanType pullPhysicalPlanType,
      final QuerySourceType querySourceType,
      final Materialization mat,
      final DataSourceOperator dataSourceOperator
  ) {
    this.root = Objects.requireNonNull(root, "root");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.lookupConstraints = Objects.requireNonNull(lookupConstraints, "lookupConstraints");
    this.pullPhysicalPlanType = Objects.requireNonNull(pullPhysicalPlanType,
        "pullPhysicalPlanType");
    this.querySourceType = Objects.requireNonNull(querySourceType, "pullSourceType");
    this.mat = Objects.requireNonNull(mat, "mat");
    this.dataSourceOperator = Objects.requireNonNull(
        dataSourceOperator, "dataSourceOperator");
  }

  public void execute(
      final List<KsqlPartitionLocation> locations,
      final PullQueryWriteStream pullQueryQueue,
      final Function<StreamedRow, StreamedRow> addDebugInfo) {

    // We only know at runtime which partitions to get from which node.
    // That's why we need to set this explicitly for the dataSource operators
    dataSourceOperator.setPartitionLocations(locations);

    open();
    QueryRow row = (QueryRow) next();

    while (!pullQueryQueue.isDone() && row != null) {
      try {
        // set a high timeout here because awaitCapacity will return early
        // if the pullQueryQueue is closed
        if (!pullQueryQueue.awaitCapacity(1, TimeUnit.SECONDS)) {
          continue;
        }
      } catch (final InterruptedException e) {
        throw new KsqlException(e);
      }

      final StreamedRow streamedRow = addDebugInfo.apply(
          StreamedRow.pullRow(GenericRow.fromList(row.value().values()), Optional.empty())
      );

      pullQueryQueue.write(ImmutableList.of(streamedRow));
      row = (QueryRow) next();
    }

    if (row != null) {
      // If the queue has been closed, we stop adding rows and cleanup. This should be triggered
      // because the client has closed their connection with the server before the results have
      // completed or the limit was hit
      LOGGER.info("Queue closed before results completed. Stopping execution.");
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

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public AbstractPhysicalOperator getRoot() {
    return root;
  }

  public Materialization getMaterialization() {
    return mat;
  }

  public List<KsqlKey> getKeys() {
    final List<KsqlKey> list = new ArrayList<>();
    for (LookupConstraint c : lookupConstraints) {
      if (c instanceof KeyConstraint) {
        final KeyConstraint kc = (KeyConstraint) c;
        list.add(kc.getKsqlKey());
      } else {
        //we shouldn't see any NonKeyContraints here
        return Collections.emptyList();
      }
    }
    return ImmutableList.copyOf(list);
  }

  public LogicalSchema getOutputSchema() {
    return schema;
  }

  public PullPhysicalPlanType getPlanType() {
    return pullPhysicalPlanType;
  }

  public QuerySourceType getSourceType() {
    return querySourceType;
  }

  public long getRowsReadFromDataSource() {
    return dataSourceOperator.getReturnedRowCount();
  }

  public QueryId getQueryId() {
    return queryId;
  }

  /**
   * The types we consider for metrics purposes. These should only be added to. You can deprecate
   * a field, but don't delete it or change its meaning
   */
  public enum PullPhysicalPlanType {
    // Could be one or more keys
    KEY_LOOKUP,
    RANGE_SCAN,
    TABLE_SCAN,
    UNKNOWN
  }
}
