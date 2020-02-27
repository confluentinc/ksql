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

package io.confluent.ksql.rest.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class KsqlClientUtilTest {

  private static final String PATH = "/ksql";
  private static final String ERROR_REASON = "something bad";

  @Mock
  private ResponseWithBody response;
  @Mock
  private HttpClientResponse httpClientResponse;
  @Mock
  private Function<ResponseWithBody, KsqlEntityList> mapper;
  @Mock
  private KsqlEntityList entities;

  @Before
  public void setUp() {
    when(response.getResponse()).thenReturn(httpClientResponse);
    when(mapper.apply(response)).thenReturn(entities);
  }

  @Test
  public void shouldCreateRestResponseFromSuccessfulResponse() {
    // Given:
    when(httpClientResponse.statusCode()).thenReturn(Code.OK.getCode());

    // When:
    final RestResponse<KsqlEntityList> restResponse =
        KsqlClientUtil.toRestResponse(response, PATH, mapper);

    // Then:
    assertThat("is successful", restResponse.isSuccessful());
    assertThat(restResponse.getStatusCode(), is(Code.OK));
    assertThat(restResponse.getResponse(), sameInstance(entities));
  }

  @Test
  public void shouldCreateRestResponseFromUnsuccessfulResponseWithMessage() {
    // Given:
    KsqlErrorMessage errorMessage = new KsqlErrorMessage(12345, "foobar");
    when(httpClientResponse.statusCode()).thenReturn(Code.BAD_REQUEST.getCode());
    when(response.getBody()).thenReturn(KsqlClientUtil.serialize(errorMessage));

    // When:
    final RestResponse<KsqlEntityList> restResponse =
        KsqlClientUtil.toRestResponse(response, PATH, mapper);

    // Then:
    assertThat("is erroneous", restResponse.isErroneous());
    assertThat(restResponse.getStatusCode(), is(Code.BAD_REQUEST));
    assertThat(restResponse.getErrorMessage(), is(errorMessage));
    verify(mapper, never()).apply(any());
  }


  @Test
  public void shouldCreateRestResponseFromNotFoundResponse() {
    // Given:
    when(httpClientResponse.statusCode()).thenReturn(Code.NOT_FOUND.getCode());

    // When:
    final RestResponse<KsqlEntityList> restResponse =
        KsqlClientUtil.toRestResponse(response, PATH, mapper);

    // Then:
    assertThat("is erroneous", restResponse.isErroneous());
    assertThat(restResponse.getStatusCode(), is(Code.NOT_FOUND));
    assertThat(restResponse.getErrorMessage().getMessage(),
        containsString(
            "Check your ksql http url to make sure you are connecting to a ksql server"));
  }

  @Test
  public void shouldCreateRestResponseFromUnauthorizedResponse() {
    // Given:
    when(httpClientResponse.statusCode()).thenReturn(Code.UNAUTHORIZED.getCode());

    // When:
    final RestResponse<KsqlEntityList> restResponse =
        KsqlClientUtil.toRestResponse(response, PATH, mapper);

    // Then:
    assertThat("is erroneous", restResponse.isErroneous());
    assertThat(restResponse.getStatusCode(), is(Code.UNAUTHORIZED));
    assertThat(restResponse.getErrorMessage().getMessage(),
        containsString("Could not authenticate successfully with the supplied credential"));
  }

  @Test
  public void shouldCreateRestResponseFromForbiddenResponse() {
    // Given:
    when(httpClientResponse.statusCode()).thenReturn(Code.FORBIDDEN.getCode());

    // When:
    final RestResponse<KsqlEntityList> restResponse =
        KsqlClientUtil.toRestResponse(response, PATH, mapper);

    // Then:
    assertThat("is erroneous", restResponse.isErroneous());
    assertThat(restResponse.getStatusCode(), is(Code.FORBIDDEN));
    assertThat(restResponse.getErrorMessage().getMessage(),
        containsString("You are forbidden from using this cluster"));
  }

  @Test
  public void shouldCreateRestResponseFromUnknownResponse() {
    // Given:
    when(httpClientResponse.statusCode()).thenReturn(Code.INTERNAL_SERVER_ERROR.getCode());
    when(httpClientResponse.statusMessage()).thenReturn(ERROR_REASON);

    // When:
    final RestResponse<KsqlEntityList> restResponse =
        KsqlClientUtil.toRestResponse(response, PATH, mapper);

    // Then:
    assertThat("is erroneous", restResponse.isErroneous());
    assertThat(restResponse.getStatusCode(), is(Code.INTERNAL_SERVER_ERROR));
    assertThat(restResponse.getErrorMessage().getMessage(),
        containsString("The server returned an unexpected error"));
    assertThat(restResponse.getErrorMessage().getMessage(),
        containsString(ERROR_REASON));
  }

  @Test
  public void shouldSerialiseDeserialise() {
    // Given:
    Map<String, Object> props = new HashMap<>();
    props.put("auto.offset.reset", "latest");
    KsqlRequest request = new KsqlRequest("some ksql", props, Collections.emptyMap(), 21345L);

    // When:
    Buffer buff = KsqlClientUtil.serialize(request);

    // Then:
    assertThat(buff, is(notNullValue()));
    String expectedJson = "{\"ksql\":\"some ksql\",\"configOverrides\":{\"auto.offset.reset\":\""
        + "latest\"},\"requestProperties\":{},\"commandSequenceNumber\":21345}";
    assertThat(new JsonObject(buff), is(new JsonObject(expectedJson)));

    // When:
    KsqlRequest deserialised = KsqlClientUtil
        .deserialize(Buffer.buffer(expectedJson), KsqlRequest.class);

    // Then:
    assertThat(deserialised, is(request));
  }

}