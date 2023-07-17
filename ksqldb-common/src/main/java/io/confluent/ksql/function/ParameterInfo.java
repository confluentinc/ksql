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

package io.confluent.ksql.function;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.function.types.ParamType;
import java.util.Objects;

@Immutable
public final class ParameterInfo {

  private final String name;
  private final ParamType type;
  private final String description;
  private final boolean isVariadic;

  public ParameterInfo(
      final String name,
      final ParamType type,
      final String description,
      final boolean isVariadic
  ) {
    this.name = name;
    this.type = type;
    this.description = description;
    this.isVariadic = isVariadic;
  }

  public String name() {
    return name;
  }

  public ParamType type() {
    return type;
  }

  public String description() {
    return description;
  }

  public boolean isVariadic() {
    return isVariadic;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ParameterInfo that = (ParameterInfo) o;
    return isVariadic == that.isVariadic
        && Objects.equals(name, that.name)
        && Objects.equals(type, that.type)
        && Objects.equals(description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, description, isVariadic);
  }

  @Override
  public String toString() {
    return "ParameterInfo{"
        + "name='" + name + '\''
        + ", type=" + type
        + ", description='" + description + '\''
        + ", isVariadic=" + isVariadic
        + '}';
  }
}
