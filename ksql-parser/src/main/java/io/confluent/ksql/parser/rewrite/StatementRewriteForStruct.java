/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.parser.rewrite;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter.Context;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.VisitParentExpressionVisitor;
import java.util.Objects;
import java.util.Optional;

public final class StatementRewriteForStruct {

  private final Statement statement;

  public StatementRewriteForStruct(final Statement statement) {
    this.statement = Objects.requireNonNull(statement, "statement");
  }

  public Statement rewriteForStruct() {
    return (Statement) new StatementRewriter<>(
        (e, c) -> ExpressionTreeRewriter.rewriteWith(new Plugin()::process, e)
    ).rewrite(statement, null);
  }

  public static boolean requiresRewrite(final Statement statement) {
    return statement instanceof Query
        || statement instanceof QueryContainer;
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
      return createFetchFunctionNodeIfNeeded(node, context);
    }

    private Optional<Expression> createFetchFunctionNodeIfNeeded(
        final DereferenceExpression dereferenceExpression,
        final Context<Void> context
    ) {
      if (dereferenceExpression.getBase() instanceof QualifiedNameReference) {
        return getNewDereferenceExpression(dereferenceExpression, context);
      }
      return getNewFunctionCall(dereferenceExpression, context);
    }

    private Optional<Expression> getNewFunctionCall(
        final DereferenceExpression dereferenceExpression,
        final Context<Void> context
    ) {
      final Expression createFunctionResult
          = context.process(dereferenceExpression.getBase());
      final String fieldName = dereferenceExpression.getFieldName();
      return Optional.of(new FunctionCall(
          QualifiedName.of("FETCH_FIELD_FROM_STRUCT"),
          ImmutableList.of(createFunctionResult, new StringLiteral(fieldName))));
    }

    private Optional<Expression> getNewDereferenceExpression(
        final DereferenceExpression dereferenceExpression,
        final Context<Void> context
    ) {
      return Optional.of(new DereferenceExpression(
          dereferenceExpression.getLocation(),
          context.process(dereferenceExpression.getBase()),
          dereferenceExpression.getFieldName()));
    }
  }

}