/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.engine;

import com.google.common.collect.Iterables;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.util.KsqlException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility for constructing {@link QueryId}s - separate from {@code EngineExecutor} for
 * easy access to unit testing.
 */
final class QueryIdUtil {

  private QueryIdUtil() {
  }

  /**
   * Builds a {@link QueryId} for a physical plan specification.
   *
   * @param metaStore   the meta store representing the current state of the engine
   * @param idGenerator generates query ids
   * @param outputNode  the logical plan
   * @param createOrReplaceEnabled whether or not the queryID can replace an existing one
   * @return the {@link QueryId} to be used
   */
  static QueryId buildId(
      final MetaStore metaStore,
      final QueryIdGenerator idGenerator,
      final OutputNode outputNode,
      final boolean createOrReplaceEnabled) {
    if (!outputNode.getSinkName().isPresent()) {
      return new QueryId(String.valueOf(Math.abs(ThreadLocalRandom.current().nextLong())));
    }

    final KsqlStructuredDataOutputNode structured = (KsqlStructuredDataOutputNode) outputNode;
    if (!structured.createInto()) {
      return new QueryId("INSERTQUERY_" + idGenerator.getNext());
    }

    final SourceName sink = outputNode.getSinkName().get();
    final Set<String> queriesForSink = metaStore.getQueriesWithSink(sink);
    if (queriesForSink.size() > 1) {
      throw new KsqlException("REPLACE for sink " + sink + " is not supported because there are "
          + "multiple queries writing into it: " + queriesForSink);
    } else if (!queriesForSink.isEmpty()) {
      if (!createOrReplaceEnabled) {
        final String type = outputNode.getNodeOutputType().getKsqlType().toLowerCase();
        throw new UnsupportedOperationException(
            String.format(
                "Cannot add %s '%s': A %s with the same name already exists",
                type,
                sink.text(),
                type));
      }
      return new QueryId(Iterables.getOnlyElement(queriesForSink));
    }

    final String suffix = outputNode.getId().toString().toUpperCase()
        + "_" + idGenerator.getNext().toUpperCase();
    return new QueryId(
        outputNode.getNodeOutputType() == DataSourceType.KTABLE
            ? "CTAS_" + suffix
            : "CSAS_" + suffix
    );
  }

}
