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

import static io.confluent.ksql.rest.Errors.ERROR_CODE_HTTP2_ONLY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.api.utils.QueryResponse;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClientOptions;
import org.junit.Test;

public class Http2OnlyStreamTest extends BaseApiTest {

  protected WebClientOptions createClientOptions() {
    return new WebClientOptions()
        .setDefaultHost("localhost")
        .setDefaultPort(server.getListeners().get(0).getPort())
        .setReusePort(true);
  }

  @Test
  public void shouldRejectInsertsUsingHttp11() throws Exception {

    // Given:
    JsonObject requestBody = new JsonObject().put("target", "test-stream");

    // Then:
    shouldRejectRequestUsingHttp11("/inserts-stream", requestBody);
  }

  private void shouldRejectRequestUsingHttp11(final String uri, final JsonObject request)
      throws Exception {
    // When
    HttpResponse<Buffer> response = sendPostRequest(uri, request.toBuffer());

    // Then
    assertThat(response.statusCode(), is(400));
    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_HTTP2_ONLY, "This endpoint is only available when using HTTP2",
        queryResponse.responseObject);
  }


}
