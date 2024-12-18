/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.api.utils.QueryResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import org.junit.Test;

public class ServerStateTest extends BaseApiTest {

  private static final int INITIALIZING_ERROR_CODE = 50333;
  private static final String INITIALIZING_MESSAGE = "waiting for preconditions";

  @Test
  public void shouldReturn503IfInitializing() throws Exception {
    // Given:
    this.serverState.setInitializingReason(new KsqlErrorMessage(INITIALIZING_ERROR_CODE, INITIALIZING_MESSAGE));

    // When/Then:
    validateResponse(503, INITIALIZING_ERROR_CODE, INITIALIZING_MESSAGE);
  }

  @Test
  public void shouldReturn503IfTerminating() throws Exception {
    // Given:
    this.serverState.setTerminating();

    // When/Then:
    validateResponse(503, Errors.ERROR_CODE_SERVER_SHUTTING_DOWN, "The server is shutting down");
  }

  @Test
  public void shouldReturn503IfTerminated() throws Exception {
    // Given:
    this.serverState.setTerminated();

    // When/Then:
    validateResponse(503, Errors.ERROR_CODE_SERVER_SHUT_DOWN, "The server is shut down");
  }

  private void validateResponse(
      final int expectedStatus,
      final int expectedErrorCode,
      final String expectedMessage
  ) throws Exception {
    // When:
    HttpResponse<Buffer> response = sendPostRequest(
        "/query-stream",
        DEFAULT_PUSH_QUERY_REQUEST_BODY.toBuffer()
    );

    // Then
    assertThat(response.statusCode(), is(expectedStatus));

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(expectedErrorCode, expectedMessage, queryResponse.responseObject);
  }
}
