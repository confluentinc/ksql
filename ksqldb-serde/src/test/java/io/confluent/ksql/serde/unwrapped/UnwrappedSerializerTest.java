package io.confluent.ksql.serde.unwrapped;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UnwrappedSerializerTest {

  private static final String TOPIC = "some-topic";
  private static final Headers HEADERS = new RecordHeaders();
  private static final byte[] SERIALIZED = "serialized".getBytes(StandardCharsets.UTF_8);
  private static final String DATA = "data";

  @Mock
  private Serializer<String> inner;
  private UnwrappedSerializer<String> serializer;

  @Before
  public void setUp() {
    serializer = new UnwrappedSerializer<>(inner, String.class);

    when(inner.serialize(any(), any())).thenReturn(SERIALIZED);
    when(inner.serialize(any(), any(), any())).thenReturn(SERIALIZED);
  }

  @Test
  public void shouldThrowIfLessThanOneValue() {
    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize("t", ImmutableList.of())
    );

    // Then:
    assertThat(e.getMessage(), is("Column count mismatch on serialization. topic: t, expected: 1, got: 0"));
  }

  @Test
  public void shouldThrowIfMoreThanOneValue() {
    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize("t", ImmutableList.of("too", "many"))
    );

    // Then:
    assertThat(e.getMessage(), is("Column count mismatch on serialization. topic: t, expected: 1, got: 2"));
  }

  @Test
  public void shouldThrowIfWrongType() {
    // Then:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize("t", ImmutableList.of(12))
    );

    // Then:
    assertThat(e.getMessage(), is("value does not match expected type. "
        + "expected: String, but got: Integer"));
  }

  @Test
  public void shouldConfigureInner() {
    // Given:
    final Map<String, ?> configs = ImmutableMap.of("this", "that");

    // When:
    serializer.configure(configs, true);

    // Then:
    verify(inner).configure(configs, true);
  }

  @Test
  public void shouldCloseInner() {
    // When:
    serializer.close();

    // Then:
    verify(inner).close();
  }

  @Test
  public void shouldSerializeOldStyleNulls() {
    // When:
    final byte[] result = serializer.serialize(TOPIC, null);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldSerializeNewStyleNulls() {
    // When:
    final byte[] result = serializer.serialize(TOPIC, HEADERS, null);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldSerializeOldStyle() {
    // Given:
    final List<?> values = ImmutableList.of(DATA);

    // When:
    final byte[] result = serializer.serialize(TOPIC, values);

    // Then:
    verify(inner).serialize(TOPIC, DATA);
    assertThat(result, is(SERIALIZED));
  }

  @Test
  public void shouldSerializeNewStyle() {
    // Given:
    final List<?> values = ImmutableList.of(DATA);

    // When:
    final byte[] result = serializer.serialize(TOPIC, HEADERS, values);

    // Then:
    verify(inner).serialize(TOPIC, HEADERS, DATA);
    assertThat(result, is(SERIALIZED));
  }
}