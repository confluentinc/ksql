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

package io.confluent.ksql.api.client.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.api.client.exception.KsqlClientException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.junit.Test;

public class HttpResponseImplTest {

  @Test
  public void testResponseHandlesNullBody() {
    HttpResponseImpl response = new HttpResponseImpl(200, null);
    assertNull(response.body());
    assertThat(response.status(), is(200));
    assertThat(response.bodyAsMap(), is(Collections.emptyMap()));
  }

  @Test
  public void testDeserializeJson() {
    HttpResponseImpl response = new HttpResponseImpl(200,
        "{'a': 10}".replace('\'', '"').getBytes(StandardCharsets.UTF_8)
    );
    assertNotNull(response.body());
    assertThat(response.status(), is(200));
    assertThat(response.bodyAsMap().get("a"), is(10));
  }

  @Test
  public void shouldConvertDeserializationErrorIntoKsqlExceptionsTest() {
    // response that cannot be deserialized into a map
    HttpResponseImpl response = new HttpResponseImpl(200,
        "hello world".getBytes(StandardCharsets.UTF_8)
    );

    assertThat(
        assertThrows(KsqlClientException.class, response::bodyAsMap).getMessage(),
        is("Could not decode response: hello world")
    );
  }

}
