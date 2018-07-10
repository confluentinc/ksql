/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class FunctionInfo {

  private final List<String> argumentTypes;
  private final String returnType;
  private final String description;

  @JsonCreator
  public FunctionInfo(
      @JsonProperty("argumentTypes") final List<String> argumentTypes,
      @JsonProperty("returnType") final String returnType,
      @JsonProperty("description") final String description
  ) {
    this.argumentTypes = Objects.requireNonNull(argumentTypes, "argumentTypes can't be null");
    this.returnType = Objects.requireNonNull(returnType, "returnType can't be null");
    this.description = Objects.requireNonNull(description, "description can't be null");
  }

  public List<String> getArgumentTypes() {
    return Collections.unmodifiableList(argumentTypes);
  }

  public String getReturnType() {
    return returnType;
  }

  public String getDescription() {
    return description;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final FunctionInfo that = (FunctionInfo) o;
    return Objects.equals(argumentTypes, that.argumentTypes)
        && Objects.equals(returnType, that.returnType)
        && Objects.equals(description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(argumentTypes, returnType, description);
  }

  @Override
  public String toString() {
    return "FunctionInfo{"
        + "argumentTypes=" + argumentTypes
        + ", returnType='" + returnType + '\''
        + ", description='" + description + '\''
        + '}';
  }
}
