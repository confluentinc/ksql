/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.ksql.types;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.utils.FormatOptions;
import java.util.Objects;

@Immutable
public final class SqlMap extends SqlType {

  private final SqlType keyType;
  private final SqlType valueType;

  public static SqlMap of(final SqlType keyType, final SqlType valueType) {
    return new SqlMap(keyType, valueType);
  }

  private SqlMap(final SqlType keyType, final SqlType valueType) {
    super(SqlBaseType.MAP);
    this.keyType = requireNonNull(keyType, "keyType");
    this.valueType = requireNonNull(valueType, "valueType");
  }

  public SqlType getKeyType() {
    return keyType;
  }

  public SqlType getValueType() {
    return valueType;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SqlMap map = (SqlMap) o;
    return Objects.equals(keyType, map.keyType)
        && Objects.equals(valueType, map.valueType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyType, valueType);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  @Override
  public String toString(final FormatOptions formatOptions) {
    return "MAP<"
        + keyType.toString(formatOptions) + ", "
        + valueType.toString(formatOptions)
        + '>';
  }
}
