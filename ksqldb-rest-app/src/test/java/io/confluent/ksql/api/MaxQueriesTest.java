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

import static io.confluent.ksql.rest.Errors.ERROR_CODE_MAX_PUSH_QUERIES_EXCEEDED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.confluent.ksql.api.utils.QueryResponse;
import io.confluent.ksql.rest.entity.PushQueryId;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import java.util.Map;
import org.junit.Test;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MaxQueriesTest extends BaseApiTest {

  protected static final Logger log = LogManager.getLogger(MaxQueriesTest.class);

  private static final int MAX_QUERIES = 10;

  @Test
  public void shouldNotCreateMoreThanMaxQueries() throws Exception {

    for (int i = 0; i < MAX_QUERIES + 4; i++) {

      if (i >= MAX_QUERIES) {
        HttpResponse<Buffer> response = sendPostRequest("/query-stream",
            DEFAULT_PUSH_QUERY_REQUEST_BODY.toBuffer());
        assertThat(response.statusCode(), is(400));
        QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
        validateError(ERROR_CODE_MAX_PUSH_QUERIES_EXCEEDED,
            "Maximum number of push queries exceeded",
            queryResponse.responseObject);
      } else {
        // When:
        QueryResponse queryResponse = executePushQueryAndWaitForRows(
            DEFAULT_PUSH_QUERY_REQUEST_BODY);
        String queryId = queryResponse.responseObject.getString("queryId");

        // Then:
        assertThat(queryId, is(notNullValue()));
        assertThat(server.getQueryIDs().contains(new PushQueryId(queryId)), is(true));
      }
    }

    assertThat(server.getQueryIDs(), hasSize(MAX_QUERIES));
  }

  @Override
  protected KsqlRestConfig createServerConfig() {
    KsqlRestConfig config = super.createServerConfig();
    Map<String, Object> origs = config.originals();
    origs.put(KsqlRestConfig.MAX_PUSH_QUERIES, MAX_QUERIES);
    return new KsqlRestConfig(origs);
  }
}
