package io.confluent.ksql.rest.server.computation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import io.confluent.ksql.rest.server.computation.ConfigTopicKey.StringKey;
import java.util.Objects;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = As.WRAPPER_OBJECT
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = StringKey.class, name = "string")
})
abstract class ConfigTopicKey {

  public static class StringKey extends ConfigTopicKey {
    private final String value;

    public StringKey(@JsonProperty("value") final String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      StringKey stringKey = (StringKey) o;
      return Objects.equals(value, stringKey.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }
  }
}
