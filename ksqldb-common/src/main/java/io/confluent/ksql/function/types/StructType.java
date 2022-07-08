/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.types;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class StructType extends ObjectType {

  /**
   * An empty struct accepts any struct as an instantiation.
   */
  public static final StructType ANY_STRUCT = StructType.builder().build();

  private final ImmutableMap<String, ParamType> schema;

  private StructType(final Map<String, ParamType> schema) {
    this.schema = ImmutableMap.copyOf(Objects.requireNonNull(schema, "schema"));
  }

  public static StructType.Builder builder() {
    return new Builder();
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "schema is ImmutableMap")
  public Map<String, ParamType> getSchema() {
    return schema;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StructType that = (StructType) o;
    return Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema);
  }

  @Override
  public String toString() {
    return "STRUCT<"
        + schema.entrySet()
            .stream()
            .map(e -> e.getKey() + " " + e.getValue())
            .collect(Collectors.joining(", "))
        + ">";
  }

  public static final class Builder {
    private final ImmutableMap.Builder<String, ParamType> builder = new ImmutableMap.Builder<>();

    private Builder() {
    }

    public Builder field(final String name, final ParamType value) {
      builder.put(name, value);
      return this;
    }

    public StructType build() {
      return new StructType(builder.build());
    }

  }

}
