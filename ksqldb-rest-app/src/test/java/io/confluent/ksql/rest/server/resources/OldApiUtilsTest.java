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

package io.confluent.ksql.rest.server.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.api.server.OldApiUtils;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import org.junit.Test;

public class OldApiUtilsTest {

  @Test
  public void shouldReturnEmbeddedResponseForKsqlRestException() {
    final EndpointResponse response = EndpointResponse.failed(400);
    assertThat(
        OldApiUtils.mapException(new KsqlRestException(response)),
        sameInstance(response));
  }

  @Test
  public void shouldReturnCorrectResponseForUnspecificException() {
    final EndpointResponse response = OldApiUtils
        .mapException(new Exception("error msg"));
    assertThat(response.getEntity(), instanceOf(KsqlErrorMessage.class));
    final KsqlErrorMessage errorMessage = (KsqlErrorMessage) response.getEntity();
    assertThat(errorMessage.getMessage(), equalTo("error msg"));
    assertThat(errorMessage.getErrorCode(), equalTo(Errors.ERROR_CODE_SERVER_ERROR));
    assertThat(response.getStatus(), equalTo(INTERNAL_SERVER_ERROR.code()));
  }
}
