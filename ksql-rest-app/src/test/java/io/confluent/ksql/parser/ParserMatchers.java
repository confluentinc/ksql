/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import org.hamcrest.Factory;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

public final class ParserMatchers {

  private ParserMatchers() {
  }

  public static <T extends Statement> Matcher<PreparedStatement<T>> preparedStatementText(
      final Matcher<? super String> textMatcher
  ) {
    return StatementTextMatcher.statementWithText(textMatcher);
  }

  public static <T extends Statement> Matcher<PreparedStatement<T>> preparedStatementText(
      final String statementText
  ) {
    return StatementTextMatcher.statementWithText(statementText);
  }

  public static <T extends Statement> Matcher<PreparedStatement<T>> preparedStatement(
      final Matcher<? super Statement> statementMatcher
  ) {
    return StatementMatcher.statement(statementMatcher);
  }

  @SuppressWarnings("unchecked")
  public static <T extends Statement> Matcher<PreparedStatement<T>> preparedStatement(
      final String statementText,
      final Class<T> statementType
  ) {
    return (Matcher) both(StatementTextMatcher.statementWithText(statementText))
        .and(StatementMatcher.statement(instanceOf(statementType)));
  }

  @SuppressWarnings("unchecked")
  public static <T extends Statement> Matcher<PreparedStatement<T>> preparedStatement(
      final Matcher<? super String> statementTextMatcher,
      final Matcher<? super Statement> statementMatcher
  ) {
    return (Matcher) both(StatementTextMatcher.statementWithText(statementTextMatcher))
        .and(StatementMatcher.statement(statementMatcher));
  }

  @SuppressWarnings("WeakerAccess")
  public static final class StatementTextMatcher<T extends Statement>
      extends FeatureMatcher<PreparedStatement<T>, String> {

    public StatementTextMatcher(Matcher<? super String> textMatcher) {
      super(textMatcher, "a prepared statement with text", "statement text");
    }

    @Override
    protected String featureValueOf(final PreparedStatement<T> actual) {
      return actual.getStatementText();
    }

    @Factory
    public static <T extends Statement> Matcher<PreparedStatement<T>> statementWithText(
        final Matcher<? super String> textMatcher
    ) {
      return new StatementTextMatcher<>(textMatcher);
    }

    @Factory
    public static <T extends Statement> Matcher<PreparedStatement<T>> statementWithText(
        final String text
    ) {
      return new StatementTextMatcher<>(is(text));
    }
  }

  @SuppressWarnings("WeakerAccess")
  public static final class StatementMatcher<T extends Statement>
      extends FeatureMatcher<PreparedStatement<T>, Statement> {

    public StatementMatcher(Matcher<? super Statement> textMatcher) {
      super(textMatcher, "a prepared statement", "statement");
    }

    @Override
    protected Statement featureValueOf(final PreparedStatement<T> actual) {
      return actual.getStatement();
    }

    @Factory
    public static <T extends Statement> Matcher<PreparedStatement<T>> statement(
        final Matcher<? super Statement> textMatcher
    ) {
      return new StatementMatcher<>(textMatcher);
    }
  }
}