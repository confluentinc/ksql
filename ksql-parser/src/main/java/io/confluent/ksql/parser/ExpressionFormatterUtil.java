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

package io.confluent.ksql.parser;

import static java.lang.String.format;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.formatter.ExpressionFormatter;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.util.ParserUtil;
import java.util.List;
import java.util.Set;

public final class ExpressionFormatterUtil {
  private ExpressionFormatterUtil() {
  }

  public static String formatExpression(final Expression expression) {
    return formatExpression(expression, true);
  }

  public static String formatExpression(final Expression expression, final boolean unmangleNames) {
    return ExpressionFormatter.formatExpression(
        expression,
        unmangleNames,
        ParserUtil::isReservedIdentifier
    );
  }

  public static String formatGroupBy(final List<GroupingElement> groupingElements) {
    final ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

    for (final GroupingElement groupingElement : groupingElements) {
      resultStrings.add(groupingElement.format());
    }
    return Joiner.on(", ").join(resultStrings.build());
  }

  public static String formatGroupingSet(final Set<Expression> groupingSet) {
    return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
            .map(ExpressionFormatterUtil::formatExpression)
            .iterator()));
  }

}
