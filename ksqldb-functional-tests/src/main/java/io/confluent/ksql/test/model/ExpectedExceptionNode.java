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

import static org.hamcrest.Matchers.instanceOf;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.tools.exceptions.KsqlExpectedException;
import io.confluent.ksql.test.tools.exceptions.MissingFieldException;
import java.util.Optional;
import org.hamcrest.Matcher;

public final class ExpectedExceptionNode {

  private final Optional<String> type;
  private final Optional<String> message;
  private final Optional<String> cause;
  private final Optional<String> causeMessage;

  ExpectedExceptionNode(
      @JsonProperty("type") final String type,
      @JsonProperty("message") final String message,
      @JsonProperty("cause") final String cause,
      @JsonProperty("causeMessage") final String causeMessage
  ) {
    this.type = Optional.ofNullable(type);
    this.message = Optional.ofNullable(message);
    this.cause = Optional.ofNullable(cause);
    this.causeMessage = Optional.ofNullable(causeMessage);

    if (!this.type.isPresent() && !this.message.isPresent()) {
      throw new MissingFieldException("expectedException.type or expectedException.message");
    }

    if (this.causeMessage.isPresent() && !this.cause.isPresent()) {
      throw new MissingFieldException("expectedException.cause");
    }
  }

  public Matcher<Throwable> build() {
    final KsqlExpectedException expectedException = KsqlExpectedException.none();

    type
        .map(ExpectedExceptionNode::parseThrowable)
        .ifPresent(expectedException::expect);

    message.ifPresent(expectedException::expectMessage);
    cause.ifPresent(c -> expectedException.expectCause(instanceOf(parseThrowable(c))));
    causeMessage.ifPresent(expectedException::expectCauseMessage);
    return expectedException.build();
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