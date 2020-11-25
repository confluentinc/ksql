/*
 * Copyright 2018 Confluent Inc.
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class GenericRow {

  private final ArrayList<Object> values;

  public GenericRow() {
    this(0);
  }

  public GenericRow(final int initialCapacity) {
    this.values = new ArrayList<>(initialCapacity);
  }

  @VisibleForTesting // Only use from tests
  public static GenericRow genericRow(final Object... columns) {
    return new GenericRow().appendAll(Arrays.asList(columns));
  }

  public static GenericRow fromList(final List<?> columns) {
    return new GenericRow().appendAll(columns);
  }

  /**
   * Ensure the row has enough capacity to hold {@code additionalCapacity} more elements than its
   * current size.
   *
   * <p>Useful to avoid unnecessary array copies when adding multiple elements.
   *
   * @param additionalCapacity the number of additional elements
   */
  public void ensureAdditionalCapacity(final int additionalCapacity) {
    values.ensureCapacity(additionalCapacity + values.size());
  }

  public int size() {
    return values.size();
  }

  public Object get(final int index) {
    return values.get(index);
  }

  public void set(final int index, final Object value) {
    values.set(index, value);
  }

  public GenericRow append(final Object value) {
    values.add(value);
    return this;
  }

  public GenericRow appendAll(final Collection<?> values) {
    this.values.addAll(values);
    return this;
  }

  @JsonProperty("columns")
  public List<Object> values() {
    return Collections.unmodifiableList(values);
  }

  @Override
  public String toString() {
    return values.stream()
        .map(GenericRow::formatValue)
        .collect(Collectors.joining(" | ", "[ ", " ]"));
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final GenericRow that = (GenericRow) o;
    return Objects.equals(this.values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }

  static String formatValue(final Object value) {
    if (value == null) {
      return "null";
    }

    if (value instanceof String) {
      return "'" + value + "'";
    }

    if (value instanceof Long) {
      return value.toString() + "L";
    }

    return value.toString();
  }
}
