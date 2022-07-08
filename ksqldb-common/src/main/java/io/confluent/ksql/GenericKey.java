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

package io.confluent.ksql;

import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Immutable
public final class GenericKey {

  @EffectivelyImmutable
  private final List<Object> values;

  public static Builder builder(final LogicalSchema schema) {
    return builder(schema.key().size());
  }

  public static Builder builder(final int numKeyColumns) {
    return new Builder(numKeyColumns);
  }

  public static GenericKey genericKey(final Object... columns) {
    return fromList(Arrays.asList(columns));
  }

  public static GenericKey fromList(final List<?> columns) {
    return builder(columns.size()).appendAll(columns).build();
  }

  public static GenericKey fromArray(final Object[] columns) {
    return fromList(Arrays.asList(columns));
  }

  private GenericKey(final List<Object> values) {
    this.values = Collections.unmodifiableList(values);
  }

  public int size() {
    return values.size();
  }

  public Object get(final int index) {
    return values.get(index);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "values is unmodifiableList()")
  public List<?> values() {
    return values;
  }

  @Override
  public String toString() {
    return values.stream()
        .map(GenericRow::formatValue)
        .collect(Collectors.joining(", ", "[", "]"));
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final GenericKey that = (GenericKey) o;
    return Objects.equals(this.values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }

  public static final class Builder {

    private final int numColumns;
    private final List<Object> values;

    private Builder(final int numColumns) {
      this.numColumns = numColumns;
      this.values = new ArrayList<>(numColumns);
    }

    public Builder append(final Object value) {
      values.add(value);
      return this;
    }

    public Builder appendAll(final List<?> values) {
      this.values.addAll(values);
      return this;
    }

    public Builder appendNulls() {
      while (values.size() < numColumns) {
        values.add(null);
      }
      return this;
    }

    public GenericKey build() {
      if (values.size() != numColumns) {
        throw new IllegalStateException("Expected " + numColumns
            + " values, only got " + values.size());
      }
      return new GenericKey(values);
    }
  }
}
