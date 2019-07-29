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
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import java.util.Objects;

public class StatementRewriteForStruct {

  private final Statement statement;

  public StatementRewriteForStruct(final Statement statement) {
    this.statement = Objects.requireNonNull(statement, "statement");
  }

  public Statement rewriteForStruct() {
    return (Statement) new RewriteWithStructFieldExtractors().process(statement, null);
  }

  public static boolean requiresRewrite(final Statement statement) {
    return statement instanceof Query
        || statement instanceof QueryContainer;
  }

  private static class RewriteWithStructFieldExtractors extends StatementRewriter {

    @Override
    public Expression visitDereferenceExpression(
        final DereferenceExpression node,
        final Object context
    ) {
      return createFetchFunctionNodeIfNeeded(node, context);
    }

    private Expression createFetchFunctionNodeIfNeeded(
        final DereferenceExpression dereferenceExpression,
        final Object context
    ) {
      if (dereferenceExpression.getBase() instanceof QualifiedNameReference) {
        return getNewDereferenceExpression(dereferenceExpression, context);
      }
      return getNewFunctionCall(dereferenceExpression, context);
    }

    private FunctionCall getNewFunctionCall(
        final DereferenceExpression dereferenceExpression,
        final Object context
    ) {
      final Expression createFunctionResult
          = (Expression) process(dereferenceExpression.getBase(), context);
      final String fieldName = dereferenceExpression.getFieldName();
      return new FunctionCall(
          QualifiedName.of("FETCH_FIELD_FROM_STRUCT"),
          ImmutableList.of(createFunctionResult, new StringLiteral(fieldName)));
    }

    private DereferenceExpression getNewDereferenceExpression(
        final DereferenceExpression dereferenceExpression,
        final Object context
    ) {
      return new DereferenceExpression(
          dereferenceExpression.getLocation(),
          (Expression) process(dereferenceExpression.getBase(), context),
          dereferenceExpression.getFieldName());
    }
  }

}