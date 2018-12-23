/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FunctionNameList extends KsqlEntity {

  private final Collection<SimpleFunctionInfo> functionNames;

  public FunctionNameList(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("functions") final Collection<SimpleFunctionInfo> functions) {
    super(statementText);
    this.functionNames = Objects.requireNonNull(functions, "functionNames can't be null");
  }

  public Collection<SimpleFunctionInfo> getFunctions() {
    return Collections.unmodifiableCollection(functionNames);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final FunctionNameList that = (FunctionNameList) o;
    return Objects.equals(functionNames, that.functionNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(functionNames);
  }
}
