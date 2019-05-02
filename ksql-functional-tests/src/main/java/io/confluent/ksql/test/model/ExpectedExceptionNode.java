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

package io.confluent.ksql.test.model;

import static org.hamcrest.Matchers.containsString;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.tools.exceptions.KsqlExpectedException;
import io.confluent.ksql.test.tools.exceptions.MissingFieldException;
import io.confluent.ksql.util.KsqlExceptionMatcher;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.Optional;

final class ExpectedExceptionNode {

  private final Optional<String> type;
  private final Optional<String> message;

  ExpectedExceptionNode(
      @JsonProperty("type") final String type,
      @JsonProperty("message") final String message
  ) {
    this.type = Optional.ofNullable(type);
    this.message = Optional.ofNullable(message);

    if (!this.type.isPresent() && !this.message.isPresent()) {
      throw new MissingFieldException("expectedException.type or expectedException.message");
    }
  }

  public KsqlExpectedException build(final String lastStatement) {
    final KsqlExpectedException expectedException = KsqlExpectedException.none();

    type
        .map(ExpectedExceptionNode::parseThrowable)
        .ifPresent(type -> {
          expectedException.expect(type);

          if (KsqlStatementException.class.isAssignableFrom(type)) {
            // Ensure exception contains last statement, otherwise the test case is invalid:
            expectedException.expect(
                KsqlExceptionMatcher.statementText(containsString(lastStatement)));
          }
        });

    message.ifPresent(expectedException::expectMessage);
    return expectedException;
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends Throwable> parseThrowable(final String className) {
    try {
      final Class<?> theClass = Class.forName(className);
      if (!Throwable.class.isAssignableFrom(theClass)) {
        throw new InvalidFieldException("expectedException.type", "Type was not a Throwable");
      }
      return (Class<? extends Throwable>) theClass;
    } catch (final ClassNotFoundException e) {
      throw new InvalidFieldException("expectedException.type", "Type was not found", e);
    }
  }

}