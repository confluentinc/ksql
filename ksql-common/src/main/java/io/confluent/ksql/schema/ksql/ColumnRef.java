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

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Objects;
import java.util.Optional;

/**
 * A reference to a column, optionally disambiguated by a qualifier indicating
 * the source of the column.
 */
@Immutable
public final class ColumnRef {

  private final Optional<SourceName> qualifier;
  private final ColumnName name;

  /**
   * Creates a {@code QualifiedName} with the following qualifier and name. A qualified name
   * can represent a disambiguation of a specific name using an additional qualifier.
   *
   * <p>For example, if two sources {@code A} and {@code B} have the same field {@code foo},
   * they can be disambiguated by referring to the field with its fully qualified name -
   * {@code A.foo} and {@code B.foo}</p>
   *
   * @param qualifier the qualifier, optionally empty
   * @param name      the name
   * @return a {@code QualifiedName} wrapping the {@code qualifier} and the {@code name}.
   */
  public static ColumnRef of(final Optional<SourceName> qualifier, final ColumnName name) {
    return new ColumnRef(qualifier, name);
  }

  /**
   * @see #of(Optional, ColumnName)
   */
  public static ColumnRef of(final SourceName qualifier, final ColumnName name) {
    return of(Optional.of(qualifier), name);
  }

  /**
   * @see #of(Optional, ColumnName)
   */
  public static ColumnRef of(final ColumnName name) {
    return new ColumnRef(Optional.empty(), name);
  }

  /**
   * @see #of(Optional, ColumnName)
   */
  @VisibleForTesting
  public static ColumnRef of(final String name) {
    return new ColumnRef(Optional.empty(), ColumnName.of(name));
  }

  private ColumnRef(final Optional<SourceName> qualifier, final ColumnName name) {
    this.qualifier = requireNonNull(qualifier, "qualifier");
    this.name = requireNonNull(name, "name");
  }

  public Optional<SourceName> qualifier() {
    return qualifier;
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
    return qualifier
        .map(q -> q.toString(formatOptions) + KsqlConstants.DOT + name.toString(formatOptions))
        .orElse(name.toString(formatOptions));
  }

  public String aliasedFieldName() {
    if (qualifier.isPresent()) {
      return SchemaUtil.buildAliasedFieldName(qualifier.get().name(), name.name());
    }

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
    return Objects.equals(qualifier, that.qualifier)
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(qualifier, name);
  }

  public ColumnRef withSource(final SourceName source) {
    return of(source, name);
  }
}
