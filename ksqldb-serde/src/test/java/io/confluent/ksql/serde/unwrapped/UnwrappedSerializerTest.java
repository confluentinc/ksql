package io.confluent.ksql.serde.unwrapped;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UnwrappedSerializerTest {

  private static final ConnectSchema SCHEMA = (ConnectSchema) SchemaBuilder.struct()
      .field("bob", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
      .build();

  private static final String TOPIC = "some-topic";
  private static final Headers HEADERS = new RecordHeaders();
  private static final byte[] SERIALIZED = "serialized".getBytes(StandardCharsets.UTF_8);
  private static final String DATA = "data";

  @Mock
  private Serializer<String> inner;
  private UnwrappedSerializer<String> serializer;

  @Before
  public void setUp() {
    serializer = new UnwrappedSerializer<>(SCHEMA, inner, String.class);

    when(inner.serialize(any(), any())).thenReturn(SERIALIZED);
    when(inner.serialize(any(), any(), any())).thenReturn(SERIALIZED);
  }

  @Test
  public void shouldThrowIfNotSingleField() {
    // Given:
    final ConnectSchema schema = (ConnectSchema) SchemaBuilder.struct()
        .field("bob", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("vic", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .build();

    // When:
    assertThrows(
        IllegalArgumentException.class,
        () -> new UnwrappedSerializer<>(schema, inner, String.class)
    );
  }

  @Test
  public void shouldThrowIfNotStruct() {
    // Given:
    final ConnectSchema wrongSchema = (ConnectSchema) SchemaBuilder.OPTIONAL_STRING_SCHEMA;

    // Then:
    assertThrows(
        IllegalArgumentException.class,
        () -> new UnwrappedSerializer<>(wrongSchema, inner, String.class)
    );
  }

  @Test
  public void shouldThrowIfSchemaDoesNotMatchTargetType() {
    // Given:
    final Serializer<Long> inner = new LongSerializer();

    // Then:
    assertThrows(
        IllegalArgumentException.class,
        () -> new UnwrappedSerializer<>(SCHEMA, inner, Long.class)
    );
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
    final Struct struct = new Struct(SCHEMA).put("bob", DATA);

    // When:
    final byte[] result = serializer.serialize(TOPIC, struct);

    // Then:
    verify(inner).serialize(TOPIC, DATA);
    assertThat(result, is(SERIALIZED));
  }

  @Test
  public void shouldSerializeNewStyle() {
    // Given:
    final Struct struct = new Struct(SCHEMA).put("bob", DATA);

    // When:
    final byte[] result = serializer.serialize(TOPIC, HEADERS, struct);

    // Then:
    verify(inner).serialize(TOPIC, HEADERS, DATA);
    assertThat(result, is(SERIALIZED));
  }
}