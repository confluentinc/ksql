/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

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

    StringKey(@JsonProperty("value") final String value) {
      this.value = Objects.requireNonNull(value, "value");
      if (this.value.isEmpty()) {
        throw new IllegalArgumentException("StringKey value must not be empty");
      }
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
      final StringKey stringKey = (StringKey) o;
      return Objects.equals(value, stringKey.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }
  }
}
