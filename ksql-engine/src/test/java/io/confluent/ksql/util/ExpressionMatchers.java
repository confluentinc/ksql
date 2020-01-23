/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.util;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public final class ExpressionMatchers {
  private ExpressionMatchers() {}

  public static Matcher<? super Expression> qualifiedNameExpression(final String value) {
    return new ExpressionMatcher<>(QualifiedColumnReferenceExp.class, value);
  }

  public static Collection<Matcher<? super Expression>> qualifiedNameExpressions(
      final String... values
  ) {
    return Arrays.stream(values)
        .map(ExpressionMatchers::qualifiedNameExpression)
        .collect(Collectors.toList());
  }

  private static class ExpressionMatcher<T extends Expression>
      extends TypeSafeDiagnosingMatcher<Expression> {

    private final String value;
    private final Class<T> type;

    private ExpressionMatcher(final Class<T> type, final String value) {
      this.value = value;
      this.type = type;
    }

    @Override
    protected boolean matchesSafely(final Expression actual, final Description description) {
      if (actual == null) {
        description.appendText("but expression was ").appendValue(null);
        return false;
      }

      if (!type.isAssignableFrom(actual.getClass())) {
        description.appendText("but expression was ").appendValue(actual);
        return false;
      }

      if (!actual.toString().equals(value)) {
        description.appendText("but value was ").appendValue(actual.toString());
        return false;
      }
      return true;
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText(type.getSimpleName() + " with name of ").appendValue(value);
    }
  }
}
