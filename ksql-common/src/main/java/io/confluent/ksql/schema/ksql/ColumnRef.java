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

package io.confluent.ksql.schema.ksql;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import java.util.Objects;

/**
 * A reference to a column.
 */
@Immutable
public final class ColumnRef {

  private final ColumnName name;

  /**
   * Creates a {@code ColumnRef} with the name.
   *
   * @param name      the name
   * @return a {@code ColumnRef} wrapping the {@code name}.
   */
  public static ColumnRef of(final ColumnName name) {
    return new ColumnRef(name);
  }

  private ColumnRef(final ColumnName name) {
    this.name = requireNonNull(name, "name");
  }

  public ColumnName name() {
    return name;
  }

  @Override
  public String toString() {
    // don't escape anything in the generic toString case
    return toString(FormatOptions.of(word -> false));
  }

  public String toString(final FormatOptions formatOptions) {
    return name.toString(formatOptions);
  }

  public String aliasedFieldName() {
    return name.name();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ColumnRef that = (ColumnRef) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
