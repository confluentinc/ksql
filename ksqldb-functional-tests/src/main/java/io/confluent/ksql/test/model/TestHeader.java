package io.confluent.ksql.test.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.Objects;
import org.apache.kafka.common.header.Header;

public class TestHeader implements Header {
  private final String key;
  private final byte[] value;

  public TestHeader(
      @JsonProperty("KEY") final String key,
      @JsonProperty("VALUE") final byte[] value
  ) {
    this.key = requireNonNull(key, "key");
    this.value = requireNonNull(value, "value");
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public byte[] value() {
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
    final TestHeader that = (TestHeader) o;
    return key.equals(that.key)
        && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }
}
