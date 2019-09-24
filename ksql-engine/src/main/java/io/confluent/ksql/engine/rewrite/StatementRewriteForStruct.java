/*
 * Copyright 2019 Confluent Inc.
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

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.QualifiedName;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.Statement;
import java.util.Optional;

public final class StatementRewriteForStruct {

  @SuppressWarnings("MethodMayBeStatic") // Used for DI
  public Statement rewriteForStruct(final Statement statement) {
    if (!requiresRewrite(statement)) {
      return statement;
    }

    final StatementRewriter<Object> rewritter = new StatementRewriter<>(
        (e, c) -> ExpressionTreeRewriter.rewriteWith(new Plugin()::process, e)
    );

    return (Statement) rewritter.rewrite(statement, null);
  }

  private static boolean requiresRewrite(final Statement statement) {
    return statement instanceof Query
        || statement instanceof QueryContainer
        || statement instanceof Explain;
  }

  private static final class Plugin
      extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    private Plugin() {
      super(Optional.empty());
    }

    @Override
    @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    public Optional<Expression> visitDereferenceExpression(
        final DereferenceExpression node,
        final Context<Void> context
    ) {
      final Expression createFunctionResult = context.process(node.getBase());
      final String fieldName = node.getFieldName();
      return Optional.of(new FunctionCall(
          QualifiedName.of("FETCH_FIELD_FROM_STRUCT"),
          ImmutableList.of(createFunctionResult, new StringLiteral(fieldName))));
    }
  }
}
