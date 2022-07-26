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

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.logicalplanner.nodes.SelectNode;
import io.confluent.ksql.logicalplanner.nodes.SourceNode;
import io.confluent.ksql.logicalplanner.nodes.StreamSourceNode;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import java.util.Objects;

/**
 * The {@code LogicalPlanner} takes a {@link Query} (ie, AST representation)
 * and converts it into a {@link LogicalPlan} that is a graph of logical
 * {@link io.confluent.ksql.logicalplanner.nodes.Node}s.
 */
public final class LogicalPlanner {

  private LogicalPlanner() {}

  public static LogicalPlan buildPlan(
      final MetaStore metaStore,
      final Query query
  ) {
    Objects.requireNonNull(metaStore, "metaStore");
    Objects.requireNonNull(query, "query");

    // cast will fail as long as we don't support joins
    // that's ok for now as we check this condition upfront
    final AliasedRelation inputStreamOrTable = (AliasedRelation) query.getFrom();

    final SourceName sourceName = inputStreamOrTable.getAlias();
    final DataSource sourceStreamOrTable = metaStore.getSource(sourceName);

    final SourceNode<?> sourceNode;
    if (sourceStreamOrTable.getDataSourceType() == DataSourceType.KSTREAM) {
      sourceNode = new StreamSourceNode(sourceName, sourceStreamOrTable.getSchema());
    } else if (sourceStreamOrTable.getDataSourceType() == DataSourceType.KTABLE) {
      throw new UnsupportedOperationException("Input TABLES are not supported by the new planner");
      //sourceNode = new TableSourceNode(sourceName, sourceStreamOrTable.getSchema());
    } else {
      throw new IllegalArgumentException("Unknown data source type "
          + sourceStreamOrTable.getDataSourceType());
    }

    final Select selectClause = query.getSelect();
    final SelectNode selectNode = new SelectNode(sourceNode, selectClause);

    return new LogicalPlan(selectNode, ImmutableSet.of(sourceName));
  }

}
