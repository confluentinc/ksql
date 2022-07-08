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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FunctionInfo {

  private final List<ArgumentInfo> arguments;
  private final String returnType;
  private final String description;

  @JsonCreator
  public FunctionInfo(
      @JsonProperty("arguments") final List<ArgumentInfo> arguments,
      @JsonProperty("returnType") final String returnType,
      @JsonProperty("description") final String description
  ) {
    this.arguments = ImmutableList.copyOf(Objects.requireNonNull(arguments, "arguments"));
    this.returnType = Objects.requireNonNull(returnType, "returnType can't be null");
    this.description = Objects.requireNonNull(description, "description can't be null");
  }

  @Deprecated // Maintained for backwards compatibility with v5.0, use getArguments() instead.
  public List<String> getArgumentTypes() {
    return arguments.stream().map(ArgumentInfo::getType).collect(Collectors.toList());
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "arguments is ImmutableList")
  public List<ArgumentInfo> getArguments() {
    return arguments;
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
    return Objects.equals(arguments, that.arguments)
        && Objects.equals(returnType, that.returnType)
        && Objects.equals(description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(arguments, returnType, description);
  }

  @Override
  public String toString() {
    return "FunctionInfo{"
        + "arguments=" + arguments
        + ", returnType='" + returnType + '\''
        + ", description='" + description + '\''
        + '}';
  }
}
