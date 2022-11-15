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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class VariablesList extends KsqlEntity {
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Variable {
    private final String name;
    private final String value;

    @JsonCreator
    public Variable(
        @JsonProperty("name") final String name,
        @JsonProperty("value") final String value
    ) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(final Object object) {
      if (this == object) {
        return true;
      }
      if (object == null || getClass() != object.getClass()) {
        return false;
      }
      final Variable that = (Variable) object;
      return Objects.equals(name, that.name)
          && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, value);
    }

    @Override
    public String toString() {
      return "Variable{"
          + "name='" + name + '\''
          + ", value='" + value + '\''
          + '}';
    }
  }

  private final List<Variable> variables;

  @JsonCreator
  public VariablesList(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("variables") final List<Variable> variables
  ) {
    super(statementText);
    this.variables = variables == null ? Collections.emptyList() : ImmutableList.copyOf(variables);
  }

  public List<Variable> getVariables() {
    return variables;
  }

  @Override
  public boolean equals(final Object o) {
    return o instanceof VariablesList
        && Objects.equals(variables, ((VariablesList)o).variables);
  }

  @Override
  public int hashCode() {
    return Objects.hash(variables);
  }
}
