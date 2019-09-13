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

package io.confluent.ksql.execution.expression.tree;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Objects;
import java.util.Optional;

/**
 * A name, optionally disambiguated by a qualifier.
 */
@Immutable
public final class QualifiedName {

  private final Optional<String> qualifier;
  private final String name;

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
  public static QualifiedName of(final Optional<String> qualifier, final String name) {
    return new QualifiedName(qualifier, name);
  }

  /**
   * @see #of(Optional, String)
   */
  public static QualifiedName of(final String qualifier, final String name) {
    return new QualifiedName(Optional.of(qualifier), name);
  }

  /**
   * @see #of(Optional, String)
   */
  public static QualifiedName of(final String name) {
    return new QualifiedName(Optional.empty(), name);
  }

  private QualifiedName(final Optional<String> qualifier, final String name) {
    this.qualifier = requireNonNull(qualifier, "qualifier");
    this.name = requireNonNull(name, "name");
  }

  public Optional<String> qualifier() {
    return qualifier;
  }

  public String name() {
    return name;
  }

  @Override
  public String toString() {
    // don't escape anything in the generic toString case
    return toString(FormatOptions.of(word -> false));
  }

  public String toString(final FormatOptions formatOptions) {
    final String escaped = formatOptions.escape(name);
    return qualifier
        .map(q -> formatOptions.escape(q) + KsqlConstants.DOT + escaped)
        .orElse(escaped);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final QualifiedName that = (QualifiedName) o;
    return Objects.equals(qualifier, that.qualifier)
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(qualifier, name);
  }
}
