/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.engine.rewrite;

import com.google.common.collect.Iterables;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/**
 * Takes a configured statement and rewrites it to update the keys requested.  This only works if
 * the query uses the IN keyword.
 * This is required because when fetching multiple keys using the IN keyword, a given host may be
 * the active for one partition and standby for another partition, and if you issue a query
 * requesting keys from both partitions while using ksql.query.pull.enable.standby.reads=true,
 * you'll fetch stale values unintentionally.
 */
public final class PullQueryKeyUpdater {

  private PullQueryKeyUpdater() {}

  /**
   * Returns a Statement with the IN expression updated to include only the given keys.
   * @param statement the original statement.
   * @param keys the keys to include
   * @return The updated statement
   */
  public static Statement update(
      final Query statement,
      final List<Struct> keys) {
    final BiFunction<Expression, Void, Expression> expressionRewriter =
        (e, v) -> ExpressionTreeRewriter.rewriteWith(
            new ExpressionRewriterPlugin(keys)::process, e, v);
    return (Statement) new StatementRewriter<>(expressionRewriter, (n, c) -> Optional.empty())
        .rewrite(statement, null);
  }

  private static final class ExpressionRewriterPlugin extends
      VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    private final List<Struct> keys;

    ExpressionRewriterPlugin(final List<Struct> keys) {
      super(Optional.empty());
      this.keys = keys;
    }

    @Override
    public Optional<Expression> visitInPredicate(final InPredicate node, final Context<Void> ctx) {
      final List<Expression> values = keys.stream().map(k -> {
        final Field field = Iterables.getOnlyElement(k.schema().fields());
        return convertToExpression(field.schema(), k.get(field));
      }).collect(Collectors.toList());
      final InListExpression inList
          = new InListExpression(node.getValueList().getLocation(), values);
      return Optional.of(new InPredicate(node.getLocation(), node.getValue(), inList));
    }

    private Expression convertToExpression(final Schema schema, final Object value) {
      switch (schema.type()) {
        case STRING:
          return new StringLiteral((String) value);
        case INT8:
        case INT16:
        case INT32:
          return new IntegerLiteral((int) value);
        case INT64:
          return new LongLiteral((long) value);
        case FLOAT32:
        case FLOAT64:
          return new DoubleLiteral((double) value);
        default:
          throw new KsqlException("Unknown key type " + schema.type());
      }
    }
  }
}
