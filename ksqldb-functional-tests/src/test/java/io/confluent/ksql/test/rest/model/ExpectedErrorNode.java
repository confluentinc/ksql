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
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.RestResponseMatchers;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessageMatchers;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.tools.exceptions.MissingFieldException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hamcrest.Matcher;

public final class ExpectedErrorNode {

  private final Optional<String> type;
  private final Optional<String> message;
  private final Optional<Integer> status;

  ExpectedErrorNode(
      @JsonProperty("type") final String type,
      @JsonProperty("message") final String message,
      @JsonProperty("status") final Integer status
  ) {
    this.type = Optional.ofNullable(type);
    this.message = Optional.ofNullable(message);
    this.status = Optional.ofNullable(status);

    if (!this.type.isPresent() && !this.message.isPresent() && !this.status.isPresent()) {
      throw new MissingFieldException(
          "expectedError.type, expectedError.message or expectedError.status");
    }
  }

  public Optional<String> getMessage() {
    return message;
  }

  public Optional<Integer> getStatus() {
    return status;
  }

  public Matcher<RestResponse<?>> build(final String lastStatement) {
    final MatcherBuilder builder = new MatcherBuilder();

    type.map(ExpectedErrorNode::parseRestError)
        .ifPresent(type -> builder.expectErrorType(type, lastStatement));

    message.ifPresent(builder::expectErrorMessage);
    status.ifPresent(builder::expectStatus);
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends KsqlErrorMessage> parseRestError(final String className) {
    try {
      final Class<?> theClass = Class.forName(className);
      if (!KsqlErrorMessage.class.isAssignableFrom(theClass)) {
        throw new InvalidFieldException("expectedError.type", "Type was not an ErrorMessage");
      }
      return (Class<? extends KsqlErrorMessage>) theClass;
    } catch (final ClassNotFoundException e) {
      throw new InvalidFieldException("expectedError.type", "Type was not found", e);
    }
  }

  private static class MatcherBuilder {

    private final List<Matcher<? super RestResponse<?>>> matchers = new ArrayList<>();

    @SuppressWarnings("unchecked")
    void expectErrorType(
        final Class<? extends KsqlErrorMessage> type,
        final String lastStatement
    ) {
      matchers.add(
          RestResponseMatchers.hasErrorMessage(
              instanceOf(type)
          )
      );

      if (KsqlStatementErrorMessage.class.isAssignableFrom(type)) {
          // Ensure exception contains last statement, otherwise the test case is invalid:
          matchers.add(
              RestResponseMatchers.hasErrorMessage(
                  (Matcher)KsqlStatementErrorMessageMatchers.statement(containsString(lastStatement))
              )
          );
      }
    }

    void expectErrorMessage(final String substring) {
      matchers.add(
          RestResponseMatchers.hasErrorMessage(
            KsqlErrorMessageMatchers.errorMessage(containsString(substring))
          )
      );
    }

    void expectStatus(final int status) {
      matchers.add(
          RestResponseMatchers.hasStatus(status)
      );
    }

    Matcher<RestResponse<?>> build() {
      return allOf(matchers);
    }
  }
}