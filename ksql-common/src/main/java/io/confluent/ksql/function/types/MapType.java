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

package io.confluent.ksql.function.types;

import java.util.Objects;

public final class MapType extends ObjectType {

  private final ParamType value;

  private MapType(ParamType value) {
    this.value = value;
  }

  public static MapType of(ParamType value) {
    return new MapType(value);
  }

  public ParamType key() {
    return ParamTypes.STRING;
  }

  public ParamType value() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MapType mapType = (MapType) o;
    return Objects.equals(value, mapType.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public String toString() {
    return "MAP<STRING, " + value + ">";
  }
}
