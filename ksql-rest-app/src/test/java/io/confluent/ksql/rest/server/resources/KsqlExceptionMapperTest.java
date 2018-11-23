/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.rest.server.resources;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.junit.Test;

public class KsqlExceptionMapperTest {
  KsqlExceptionMapper exceptionMapper = new KsqlExceptionMapper();

  @Test
  public void shouldReturnEmbeddedResponseForKsqlRestException() {
    final Response response = Response.status(400).build();
    assertThat(
        exceptionMapper.toResponse(new KsqlRestException(response)),
        sameInstance(response));
  }

  @Test
  public void shouldReturnCorrectResponseForWebAppException() {
    final WebApplicationException webApplicationException = new WebApplicationException("error msg", 403);
    final Response response = exceptionMapper.toResponse(webApplicationException);
    assertThat(response.getEntity(), instanceOf(KsqlErrorMessage.class));
    final KsqlErrorMessage errorMessage = (KsqlErrorMessage)response.getEntity();
    assertThat(errorMessage.getMessage(), equalTo("error msg"));
    assertThat(errorMessage.getErrorCode(), equalTo(40300));
    assertThat(response.getStatus(), equalTo(403));
  }

  @Test
  public void shouldReturnCorrectResponseForUnspecificException() {
    final Response response = exceptionMapper.toResponse(new Exception("error msg"));
    assertThat(response.getEntity(), instanceOf(KsqlErrorMessage.class));
    final KsqlErrorMessage errorMessage = (KsqlErrorMessage)response.getEntity();
    assertThat(errorMessage.getMessage(), equalTo("error msg"));
    assertThat(errorMessage.getErrorCode(), equalTo(Errors.ERROR_CODE_SERVER_ERROR));
    assertThat(response.getStatus(), equalTo(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()));
  }
}
