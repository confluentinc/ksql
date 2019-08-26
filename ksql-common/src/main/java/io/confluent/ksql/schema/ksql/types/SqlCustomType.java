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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import java.util.Objects;

@Immutable
public final class SqlCustomType extends SqlType {

  private final String name;

  public static SqlCustomType of(final String name) {
    return new SqlCustomType(name);
  }

  private SqlCustomType(final String name) {
    super(SqlBaseType.CUSTOM);
    this.name = Objects.requireNonNull(name, "name").toUpperCase();
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean supportsCast() {
    return false;
  }

  @Override
  public String toString(final FormatOptions formatOptions) {
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
    final SqlCustomType that = (SqlCustomType) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
