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

package io.confluent.ksql.schema.ksql.types;

import static io.confluent.ksql.util.Identifiers.ensureTrimmed;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.util.Identifiers;
import java.util.Objects;

@Immutable
public final class Field {

  private final String name;
  private final SqlType type;

  public static Field of(final String name, final SqlType type) {
    return new Field(name, type);
  }

  private Field(final String name, final SqlType type) {
    this.name = ensureTrimmed(Objects.requireNonNull(name, "name"), "name");
    this.type = Objects.requireNonNull(type, "type");
  }

  public SqlType type() {
    return type;
  }

  public String name() {
    return name;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Field that = (Field) o;
    return Objects.equals(name, that.name)
        && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  public String toString(final FormatOptions formatOptions) {
    return Identifiers.escape(name, formatOptions) + " " + type.toString(formatOptions);
  }

}
