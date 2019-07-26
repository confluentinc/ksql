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

package io.confluent.ksql.parser.tree;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.ExpressionFormatter;
import java.util.Optional;

/**
 * Expressions are used to declare select items, where and having clauses and join criteria in
 * queries.
 */
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
