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

package io.confluent.ksql.test.rest.model;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessageMatchers;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.tools.exceptions.MissingFieldException;
import io.confluent.rest.entities.ErrorMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hamcrest.Matcher;

public final class ExpectedErrorNode {

  private final Optional<String> type;
  private final Optional<String> message;

  ExpectedErrorNode(
      @JsonProperty("type") final String type,
      @JsonProperty("message") final String message
  ) {
    this.type = Optional.ofNullable(type);
    this.message = Optional.ofNullable(message);

    if (!this.type.isPresent() && !this.message.isPresent()) {
      throw new MissingFieldException("expectedException.type or expectedException.message");
    }
  }

  public Matcher<ErrorMessage> build(final String lastStatement) {
    final MatcherBuilder builder = new MatcherBuilder();

    type
        .map(ExpectedErrorNode::parseRestError)
        .ifPresent(type -> {
          builder.expect(type);

          if (KsqlStatementErrorMessage.class.isAssignableFrom(type)) {
            // Ensure exception contains last statement, otherwise the test case is invalid:
            builder
                .expect(KsqlStatementErrorMessageMatchers.statement(containsString(lastStatement)));
          }
        });

    message.ifPresent(builder::expectMessage);
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends ErrorMessage> parseRestError(final String className) {
    try {
      final Class<?> theClass = Class.forName(className);
      if (!ErrorMessage.class.isAssignableFrom(theClass)) {
        throw new InvalidFieldException("expectedError.type", "Type was not an ErrorMessage");
      }
      return (Class<? extends ErrorMessage>) theClass;
    } catch (final ClassNotFoundException e) {
      throw new InvalidFieldException("expectedError.type", "Type was not found", e);
    }
  }

  private static class MatcherBuilder {

    private final List<Matcher<?>> matchers = new ArrayList<>();

    void expect(final Class<? extends ErrorMessage> type) {
      matchers.add(instanceOf(type));
    }

    void expect(final Matcher<?> matcher) {
      matchers.add(matcher);
    }

    void expectMessage(final String substring) {
      matchers.add(KsqlErrorMessageMatchers.errorMessage(containsString(substring)));
    }

    @SuppressWarnings("unchecked")
    Matcher<ErrorMessage> build() {
      return allOf((List) new ArrayList<>(matchers));
    }
  }
}