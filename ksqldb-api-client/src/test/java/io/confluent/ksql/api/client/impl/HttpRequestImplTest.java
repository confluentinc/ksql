package io.confluent.ksql.api.client.impl;

import static org.junit.Assert.assertEquals;

import com.google.common.testing.EqualsTester;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class HttpRequestImplTest {

  private HttpRequestImpl request;

  @Before
  public void setup() {
    request = new HttpRequestImpl();
  }

  @Test
  public void testSetAndUpdateMethod() {
    // set a value for method
    request.method("get");
    assertEquals("get", request.method());

    // updating it should reflect new value
    request.method("post");
    assertEquals("post", request.method());

    new EqualsTester()
        .addEqualityGroup(new HttpRequestImpl())
        .addEqualityGroup(request, new HttpRequestImpl().post())
        .addEqualityGroup(new HttpRequestImpl().method("delete"), new HttpRequestImpl().delete())
        .addEqualityGroup(new HttpRequestImpl().method("put"), new HttpRequestImpl().put())
        .addEqualityGroup(new HttpRequestImpl().method("get"), new HttpRequestImpl().get())
        .testEquals();
  }

  @Test(expected = NullPointerException.class)
  public void nullMethodsShouldNotBeAllowedTest() {
    request.method(null);
  }

  @Test(expected = NullPointerException.class)
  public void nullPayloadKeysShouldNotBeAllowed() {
    request.payload(null, "");
  }

  @Test(expected = NullPointerException.class)
  public void nullPayloadValuesShouldNotBeAllowed() {
    request.payload("abc", null);
  }

  @Test
  public void testSetAndUpdatePayload() {
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
    request.payload(payload);
    assertEquals("two", request.payload().get("ob"));
    assertEquals("three", request.payload().get("three"));

    new EqualsTester()
        .addEqualityGroup(request)
        .addEqualityGroup(new HttpRequestImpl())
        .addEqualityGroup(
            new HttpRequestImpl().payload("one", "one").payload("two", "two"),
            new HttpRequestImpl().payload("two", "two").payload("one", "one")
        ).addEqualityGroup(new HttpRequestImpl().payload("one", "one"))
        .testEquals();
  }

  @Test
  public void testSetAndUpdatePath() {
    request.path("/a/b/c");
    assertEquals("/a/b/c", request.path());

    request.path("/x/y/z");
    assertEquals("/x/y/z", request.path());

    new EqualsTester()
        .addEqualityGroup(request, new HttpRequestImpl().path("/x/y/z"))
        .addEqualityGroup(new HttpRequestImpl())
        .testEquals();
  }
}
