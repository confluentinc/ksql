package io.confluent.ksql.serde.unwrapped;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UnwrappedDeserializerTest {

  private static final String TOPIC = "some-topic";
  private static final Headers HEADERS = new RecordHeaders();
  private static final byte[] SERIALIZED = "data".getBytes(StandardCharsets.UTF_8);
  private static final String DESERIALIZED = "deserialized";

  @Mock
  private Deserializer<String> inner;
  private UnwrappedDeserializer deserializer;

  @Before
  public void setUp() {
    deserializer = new UnwrappedDeserializer(inner);

    when(inner.deserialize(any(), any())).thenReturn(DESERIALIZED);
    when(inner.deserialize(any(), any(), (byte[]) any())).thenReturn(DESERIALIZED);
  }

  @Test
  public void shouldConfigureInner() {
    // Given:
    final Map<String, ?> configs = ImmutableMap.of("this", "that");

    // When:
    deserializer.configure(configs, true);

    // Then:
    verify(inner).configure(configs, true);
  }

  @Test
  public void shouldCloseInner() {
    // When:
    deserializer.close();

    // Then:
    verify(inner).close();
  }

  @Test
  public void shouldDeserializeOldStyleNulls() {
    // When:
    final List<?> result = deserializer.deserialize(TOPIC, null);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldDeserializeNewStyleNulls() {
    // When:
    final List<?> result = deserializer.deserialize(TOPIC, HEADERS, (byte[]) null);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldDeserializeOldStyle() {
    // When:
    final List<?> result = deserializer.deserialize(TOPIC, SERIALIZED);

    // Then:
    verify(inner).deserialize(TOPIC, SERIALIZED);
    assertThat(result, contains(DESERIALIZED));
  }

  @Test
  public void shouldDeserializeNewStyle() {
    // When:
    final List<?> result = deserializer.deserialize(TOPIC, HEADERS, SERIALIZED);

    // Then:
    verify(inner).deserialize(TOPIC, HEADERS, SERIALIZED);
    assertThat(result, contains(DESERIALIZED));
  }
}