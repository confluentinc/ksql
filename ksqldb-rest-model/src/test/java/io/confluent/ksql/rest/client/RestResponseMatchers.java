/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.client;

import static org.hamcrest.Matchers.is;

import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

public final class RestResponseMatchers {

  private RestResponseMatchers() {
  }

  public static Matcher<? super RestResponse<?>> hasStatus(final int status) {
    return new FeatureMatcher<RestResponse<?>, Integer>
        (is(status), "response with status", "status") {
      @Override
      protected Integer featureValueOf(final RestResponse<?> actual) {
        return actual.getStatusCode();
      }
    };
  }

  public static Matcher<? super RestResponse<?>> hasErrorMessage(
      final Matcher<? super KsqlErrorMessage> errorMessage
  ) {
    return new FeatureMatcher<RestResponse<?>, KsqlErrorMessage>
        (errorMessage, "response with error message", "error message") {
      @Override
      protected KsqlErrorMessage featureValueOf(final RestResponse<?> actual) {
        return actual.getErrorMessage();
      }
    };
  }
}
