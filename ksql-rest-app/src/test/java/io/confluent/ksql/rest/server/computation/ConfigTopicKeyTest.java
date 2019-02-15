package io.confluent.ksql.rest.server.computation;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.rest.server.computation.ConfigTopicKey.StringKey;
import io.confluent.ksql.rest.util.InternalTopicJsonSerdeUtil;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;

public class ConfigTopicKeyTest {
  private final StringKey stringKey = new StringKey("string-key-value");
  private final byte[] serialized
      = "{\"string\":{\"value\":\"string-key-value\"}}".getBytes(StandardCharsets.UTF_8);
  private final Serializer<ConfigTopicKey> serializer
      = InternalTopicJsonSerdeUtil.getJsonSerializer(false);
  private final Deserializer<ConfigTopicKey> deserializer
      = InternalTopicJsonSerdeUtil.getJsonDeserializer(ConfigTopicKey.class, false);


  @Test
  public void shouldImplementEqualsForStringKey() {
    new EqualsTester()
        .addEqualityGroup(new StringKey("foo"), new StringKey("foo"))
        .addEqualityGroup(new StringKey("bar"))
        .testEquals();
  }

  @Test
  public void shouldSerializeStringKey() {
    // When:
    final byte[] bytes = serializer.serialize("", stringKey);

    // Then:
    assertThat(bytes, equalTo(serialized));
  }

  @Test
  public void shouldDeserializeStringKey() {
    // When:
    final ConfigTopicKey key = deserializer.deserialize("", serialized);

    // Then:
    assertThat(key, equalTo(stringKey));
  }

  @Test
  public void shouldDeserializeStringKeyWithNoValue() {
    // When:
    final ConfigTopicKey key = deserializer.deserialize(
        "",
        "{\"string\":{}}".getBytes(StandardCharsets.UTF_8));

    // Then:
    assertThat(key, instanceOf(StringKey.class));
    assertThat(((StringKey) key).getValue(), is(nullValue()));
  }
}