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

package io.confluent.ksql.name;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.util.Identifiers;
import java.util.Objects;

/**
 * The base type for all names, which just wraps a String in
 * a type-safe wrapper and supplies formatting options.
 *
 * @param <T> ensure type safety of methods
 */
@Immutable
public abstract class Name<T extends Name<?>> {
  protected final String name;

  protected Name(final String name) {
    this.name = Identifiers.ensureTrimmed(Objects.requireNonNull(name, "name"), "name");
  }

  // we should remove this getter after all code has
  // migrated to use Name instead of Strings to make
  // sure that we never lose type safety
  @JsonValue
  public String text() {
    return name;
  }

  public boolean startsWith(final T o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return name.startsWith(o.text());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Name<?> that = (Name<?>) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), name);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  public String toString(final FormatOptions formatOptions) {
    return formatOptions.escape(name);
  }
}
