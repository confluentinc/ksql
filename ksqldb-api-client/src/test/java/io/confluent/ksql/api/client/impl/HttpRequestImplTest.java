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

package io.confluent.ksql.api.client.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.testing.EqualsTester;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class HttpRequestImplTest {

  @Test
  public void testSetMethodAndPath() {
    HttpRequestImpl request = new HttpRequestImpl("GET", "/");
    assertEquals("GET", request.method());
    assertEquals("/", request.path());
  }

  @Test
  public void nullMethodsShouldNotBeAllowedTest() {
    assertThrows(
        NullPointerException.class, () -> new HttpRequestImpl(null, "/")
    );
  }

  @Test
  public void nullPathsShouldNotBeAllowedTest() {
    assertThrows(
        NullPointerException.class, () -> new HttpRequestImpl("GET", null)
    );
  }

  @Test
  public void nullPayloadKeysShouldNotBeAllowed() {
    assertThrows(
        NullPointerException.class,
        () -> new HttpRequestImpl("GET", "/").payload(null, "")
    );
  }

  @Test
  public void nullPayloadValuesShouldNotBeAllowed() {
    assertThrows(
        NullPointerException.class,
        () -> new HttpRequestImpl("GET", "/").payload("", null)
    );
  }

  @Test
  public void testSetAndUpdatePayload() {
    HttpRequestImpl request = new HttpRequestImpl("GET", "/");
    request.payload("hello", "world");
    assertEquals(Collections.singletonMap("hello", "world"), request.payload());
    request.payload("hello", "again");
    assertEquals(Collections.singletonMap("hello", "again"), request.payload());
    request.payload("ob", "one");
    request.payload("ken", "ob");
    assertEquals(3, request.payload().size());
    assertEquals("one", request.payload().get("ob"));
    assertEquals("ob", request.payload().get("ken"));
    assertEquals("again", request.payload().get("hello"));

    Map<String, Object> payload = new HashMap<>();
    payload.put("ob", "two");
    payload.put("three", "three");
    request.payload(payload).payload("four", 4);
    assertEquals("two", request.payload().get("ob"));
    assertEquals("three", request.payload().get("three"));
    assertEquals(4, request.payload().get("four"));

    new EqualsTester()
        .addEqualityGroup(request)
        .addEqualityGroup(request())
        .addEqualityGroup(
            request().payload("one", "one").payload("two", "two"),
            request().payload("two", "two").payload("one", "one")
        ).addEqualityGroup(request().payload("one", "one"))
        .testEquals();
  }

  @Test
  public void testSetAndUpdateProperties() {
    HttpRequestImpl request = new HttpRequestImpl("GET", "/");
    request.property("hello", "world");
    assertEquals(Collections.singletonMap("hello", "world"), request.properties());
    request.property("hello", "again");
    assertEquals(Collections.singletonMap("hello", "again"), request.properties());
    request.property("ob", "one");
    request.property("ken", "ob");
    assertEquals(3, request.properties().size());
    assertEquals("one", request.properties().get("ob"));
    assertEquals("ob", request.properties().get("ken"));
    assertEquals("again", request.properties().get("hello"));

    Map<String, Object> props = new HashMap<>();
    props.put("ob", "two");
    props.put("three", "three");
    request.properties(props).property("four", 4);
    assertEquals("two", request.properties().get("ob"));
    assertEquals("three", request.properties().get("three"));
    assertEquals(4, request.properties().get("four"));

    new EqualsTester()
        .addEqualityGroup(request)
        .addEqualityGroup(request())
        .addEqualityGroup(
            request().property("one", "one").property("two", "two"),
            request().property("two", "two").property("one", "one")
        ).addEqualityGroup(request().property("one", "one"))
        .testEquals();
  }

  HttpRequestImpl request() {
    return new HttpRequestImpl("GET", "/");
  }
}
