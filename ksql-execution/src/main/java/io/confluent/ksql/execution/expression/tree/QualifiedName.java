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
import io.confluent.ksql.util.KsqlConstants;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

@Immutable
public final class QualifiedName {

  private final Optional<String> qualifier;
  private final String name;

  public static QualifiedName of(final String qualifier, final String name) {
    return new QualifiedName(Optional.of(qualifier), name);
  }

  public static QualifiedName of(final Optional<String> qualifier, final String name) {
    return new QualifiedName(qualifier, name);
  }

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
    return toString(Function.identity());
  }

  public String toString(final Function<String, String> escapeFun) {
    final String escaped = escapeFun.apply(name);
    return qualifier
        .map(q -> escapeFun.apply(q) + KsqlConstants.DOT + escaped)
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
