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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.api.server.protocol.PojoCodec;
import io.confluent.ksql.api.server.protocol.PojoDeserializerErrorHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PojoCodecTest {

  @Mock
  private PojoDeserializerErrorHandler errorHandler;

  @Test
  public void testDeserializePojo() {
    JsonObject jsonObject = new JsonObject().put("field1", 123)
        .put("field2", "foobar")
        .put("field3", true);
    Buffer buff = jsonObject.toBuffer();
    Optional<TestPojo> testPojo = PojoCodec.deserialiseObject(buff, errorHandler, TestPojo.class);
    assertThat(testPojo.isPresent(), is(true));
    assertThat(testPojo.get().field1, is(123));
    assertThat(testPojo.get().field2, is("foobar"));
    assertThat(testPojo.get().field3, is(true));
  }

  @Test
  public void testDeserializeInvalidJson() {
    Buffer buff = Buffer.buffer("{\"foo\":123");
    Optional<TestPojo> testPojo = PojoCodec.deserialiseObject(buff, errorHandler, TestPojo.class);
    assertThat(testPojo.isPresent(), is(false));
    verify(errorHandler).onInvalidJson();
  }

  @Test
  public void testDeserializeMissingField() {
    Buffer buff = Buffer.buffer("{\"field1\":123,\"field2\":\"foo\"}");
    Optional<TestPojo> testPojo = PojoCodec.deserialiseObject(buff, errorHandler, TestPojo.class);
    assertThat(testPojo.isPresent(), is(false));
    verify(errorHandler).onMissingParam("field3");
  }

  @Test
  public void testDeserializeUnknownField() {
    Buffer buff = Buffer.buffer("{\"field1\":123,\"field2\":\"foo\",\"field3\":true,\"blah\":432}");
    Optional<TestPojo> testPojo = PojoCodec.deserialiseObject(buff, errorHandler, TestPojo.class);
    assertThat(testPojo.isPresent(), is(false));
    verify(errorHandler).onExtraParam("blah");
  }

  public static class TestPojo {

    public final int field1;
    public final String field2;
    public final boolean field3;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public TestPojo(final @JsonProperty(value = "field1", required = true) int field1,
        final @JsonProperty(value = "field2", required = true) String field2,
        final @JsonProperty(value = "field3", required = true) boolean field3) {
      this.field1 = field1;
      this.field2 = field2;
      this.field3 = field3;
    }
  }

}
