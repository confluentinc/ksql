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

package io.confluent.ksql.rest.util;

import io.confluent.ksql.rest.Errors;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.TopicAuthorizationException;


public final class ErrorResponseUtil {

  private ErrorResponseUtil() {
  }

  public static Response generateResponse(
      final Exception e,
      final Response defaultResponse,
      final Errors errorHandler
  ) {
    if (ExceptionUtils.indexOfType(e, TopicAuthorizationException.class) >= 0) {
      return errorHandler.accessDeniedFromKafka(e);
    } else {
      return defaultResponse;
    }
  }
}
