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

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.engine.rewrite.DataSourceExtractor;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.tree.AstNode;
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

public class DeprecatedStatementsChecker {
  public enum Deprecations {
    DEPRECATED_STREAM_STREAM_OUTER_JOIN_WITH_NO_GRACE(
        "DEPRECATION NOTICE: Left/Outer stream-stream joins statements without a GRACE PERIOD "
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
    if (isLeftOrOuterStreamStreamJoinWithoutGraceStatement(statement)) {
      return Optional.of(Deprecations.DEPRECATED_STREAM_STREAM_OUTER_JOIN_WITH_NO_GRACE);
    }

    return Optional.empty();
  }

  private boolean isLeftOrOuterStreamStreamJoinWithoutGraceStatement(final Statement statement) {
    if (statement instanceof CreateAsSelect) {
      return isLeftOrOuterStreamStreamJoinWithoutGraceQuery(
          ((CreateAsSelect) statement).getQuery());
    } else if (statement instanceof Query) {
      return isLeftOrOuterStreamStreamJoinWithoutGraceQuery((Query) statement);
    }

    return false;
  }

  private boolean isLeftOrOuterStreamStreamJoinWithoutGraceQuery(final Query query) {
    if (query.getFrom() instanceof Join) {
      final Join join = (Join) query.getFrom();

      // check left joined source is a stream
      if (!isStream(join.getLeft())) {
        return false;
      }

      for (final JoinedSource joinedSource : join.getRights()) {
        final JoinedSource.Type joinType = joinedSource.getType();
        if (joinType == JoinedSource.Type.LEFT || joinType == JoinedSource.Type.OUTER) {
          // check right joined source is a stream
          if (!isStream(joinedSource.getRelation())) {
            break;
          }

          if (!joinedSource.getWithinExpression().flatMap(WithinExpression::getGrace).isPresent()) {
            return true;
          }
        }
      }
    }

    return false;
  }

  private boolean isStream(final Relation relation) {
    for (final Analysis.AliasedDataSource source : extractDataSources(relation)) {
      if (source.getDataSource().getDataSourceType() != DataSource.DataSourceType.KSTREAM) {
        return false;
      }
    }

    return true;
  }

  private Set<Analysis.AliasedDataSource> extractDataSources(final AstNode node) {
    final DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore, false);
    dataSourceExtractor.extractDataSources(node);
    return dataSourceExtractor.getAllSources();
  }
}
