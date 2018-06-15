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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

public class DescribeFunctionList extends KsqlEntity {

  private final Collection<FunctionInfo> functions;

  public DescribeFunctionList(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("functions") final Collection<FunctionInfo> functions) {
    super(statementText);
    this.functions = Objects.requireNonNull(functions, "functions can't be null");
  }

  public Collection<FunctionInfo> getFunctions() {
    return Collections.unmodifiableCollection(functions);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DescribeFunctionList that = (DescribeFunctionList) o;
    return Objects.equals(functions, that.functions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(functions);
  }
}
