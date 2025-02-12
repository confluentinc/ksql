/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.server.computation;

import static io.confluent.ksql.analyzer.Analysis.AliasedDataSource;

import io.confluent.ksql.engine.rewrite.DataSourceExtractor;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinedSource;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.WithinExpression;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeprecatedStatementsChecker {
  private static final Logger LOG = LoggerFactory.getLogger(DeprecatedStatementsChecker.class);

  public enum Deprecations {
    DEPRECATED_STREAM_STREAM_JOIN_WITH_NO_GRACE(
        "DEPRECATION NOTICE: Stream-stream joins statements without a GRACE PERIOD "
            + "will not be accepted in a future ksqlDB version.\n"
            + "Please use the GRACE PERIOD clause as specified in "
            + "https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/"
            + "select-push-query/"
    );

    private final String noticeMessage;

    Deprecations(final String noticeMessage) {
      this.noticeMessage = noticeMessage;
    }

    public String getNoticeMessage() {
      return noticeMessage;
    }
  }

  final MetaStore metaStore;

  DeprecatedStatementsChecker(final MetaStore metaStore) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
  }

  public Optional<Deprecations> checkStatement(final Statement statement) {
    if (isStreamStreamJoinWithoutGraceStatement(statement)) {
      return Optional.of(Deprecations.DEPRECATED_STREAM_STREAM_JOIN_WITH_NO_GRACE);
    }

    return Optional.empty();
  }

  private boolean isStreamStreamJoinWithoutGraceStatement(final Statement statement) {
    if (statement instanceof CreateAsSelect) {
      return isStreamStreamJoinWithoutGraceQuery(((CreateAsSelect) statement).getQuery());
    } else if (statement instanceof Query) {
      return isStreamStreamJoinWithoutGraceQuery((Query) statement);
    }

    return false;
  }

  private boolean isStreamStreamJoinWithoutGraceQuery(final Query query) {
    if (query.getFrom() instanceof Join) {
      final Join join = (Join) query.getFrom();

      // check left joined source is a stream
      if (!isStream(join.getLeft())) {
        return false;
      }

      for (final JoinedSource joinedSource : join.getRights()) {
        // check right joined source is a stream
        if (!isStream(joinedSource.getRelation())) {
          continue;
        }

        if (!joinedSource.getWithinExpression().flatMap(WithinExpression::getGrace).isPresent()) {
          return true;
        }
      }
    }

    return false;
  }

  private boolean isStream(final Relation relation) {
    // DataSourceExtractor must be initialized everytime we need to extract data from a node
    // because it changes its internal state to keep all sources found
    final DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);

    // The relation object should have only one joined source, but the extract sources method
    // returns a list. This loop checks all returned values are a Stream just to prevent throwing
    // an exception if more than one value is returned.
    final Set<AliasedDataSource> sources = dataSourceExtractor.extractDataSources(relation);
    if (sources.size() > 1) {
      // Just log a warning in case the relation object has more han one source
      LOG.warn("The deprecation statement checker has detected an internal join source with more "
          + "than one relation. This might be a bug in the deprecation checker or other part of "
          + " the code. Report this bug to the ksqlDB support team.");
    }

    for (final AliasedDataSource source : sources) {
      if (source.getDataSource().getDataSourceType() != DataSource.DataSourceType.KSTREAM) {
        return false;
      }
    }

    return true;
  }
}
