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

package io.confluent.ksql.execution.expression.tree;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.formatter.ExpressionFormatter;
import io.confluent.ksql.parser.Node;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Optional;

/**
 * Expressions are used to declare select items, where and having clauses and join criteria in
 * queries.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = As.WRAPPER_OBJECT
)
@JsonSubTypes({
    @Type(value = ArithmeticBinaryExpression.class, name = "ArithmeticBinaryExpression"),
    @Type(value = ArithmeticUnaryExpression.class, name = "ArithmeticUnaryExpression"),
    @Type(value = BetweenPredicate.class, name = "BetweenPredicate"),
    @Type(value = BooleanLiteral.class, name = "BooleanLiteral"),
    @Type(value = Cast.class, name = "Cast"),
    @Type(value = ComparisonExpression.class, name = "ComparisonExpression"),
    @Type(value = DecimalLiteral.class, name = "DecimalLiteral"),
    @Type(value = DereferenceExpression.class, name = "DereferenceExpression"),
    @Type(value = DoubleLiteral.class, name = "DoubleLiteral"),
    @Type(value = FunctionCall.class, name = "FunctionCall"),
    @Type(value = InListExpression.class, name = "InListExpression"),
    @Type(value = InPredicate.class, name = "InPredicate"),
    @Type(value = IntegerLiteral.class, name = "IntegerLiteral"),
    @Type(value = IsNotNullPredicate.class, name = "IsNotNullPredicate"),
    @Type(value = IsNullPredicate.class, name = "IsNullPredicate"),
    @Type(value = LikePredicate.class, name = "LikePredicate"),
    @Type(value = LogicalBinaryExpression.class, name = "LogicalBinaryExpression"),
    @Type(value = LongLiteral.class, name = "LongLiteral"),
    @Type(value = NotExpression.class, name = "NotExpression"),
    @Type(value = NullLiteral.class, name = "NullLiteral"),
    @Type(value = QualifiedName.class, name = "QualifiedName"),
    @Type(value = QualifiedNameReference.class, name = "QualifiedNameReference"),
    @Type(value = SearchedCaseExpression.class, name = "SearchedCaseExpression"),
    @Type(value = SimpleCaseExpression.class, name = "SimpleCaseExpression"),
    @Type(value = StringLiteral.class, name = "StringLiteral"),
    @Type(value = SubscriptExpression.class, name = "SubscriptExpression"),
    @Type(value = TimeLiteral.class, name = "TimeLiteral"),
    @Type(value = TimestampLiteral.class, name = "TimestampLiteral"),
    @Type(value = io.confluent.ksql.execution.expression.tree.Type.class, name = "Type"),
    @Type(value = WhenClause.class, name = "WhenClause")
})
@Immutable
public abstract class Expression extends Node {

  protected Expression(final Optional<NodeLocation> location) {
    super(location);
  }

  protected abstract <R, C> R accept(ExpressionVisitor<R, C> visitor, C context);

  @Override
  public final String toString() {
    return ExpressionFormatter.formatExpression(this);
  }
}
