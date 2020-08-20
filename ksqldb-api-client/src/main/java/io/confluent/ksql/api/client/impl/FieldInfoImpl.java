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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.FieldInfo;
import java.util.Objects;

public final class FieldInfoImpl implements FieldInfo {

  private final String name;
  private final ColumnType type;
  private final boolean isKey;

  FieldInfoImpl(final String name, final ColumnType type, final boolean isKey) {
    this.name = Objects.requireNonNull(name, "name");
    this.type = Objects.requireNonNull(type, "type");
    this.isKey = isKey;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public ColumnType type() {
    return type;
  }

  @Override
  public boolean isKey() {
    return isKey;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final FieldInfoImpl fieldInfo = (FieldInfoImpl) o;
    return name.equals(fieldInfo.name)
        && type.equals(fieldInfo.type)
        && isKey == fieldInfo.isKey;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, isKey);
  }

  @Override
  public String toString() {
    return "FieldInfo{"
        + "name='" + name + '\''
        + ", type=" + type
        + ", isKey=" + isKey
        + '}';
  }
}
