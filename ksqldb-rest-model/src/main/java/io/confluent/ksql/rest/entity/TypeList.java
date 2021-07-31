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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Objects;

@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public class TypeList extends KsqlEntity {

  private final ImmutableMap<String, SchemaInfo> types;

  @JsonCreator
  public TypeList(
      @JsonProperty("statementText")  final String statementText,
      @JsonProperty("types")          final Map<String, SchemaInfo> types
  ) {
    super(statementText);
    this.types = ImmutableMap.copyOf(Objects.requireNonNull(types, "types"));
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "types is ImmutableMap")
  public Map<String, SchemaInfo> getTypes() {
    return types;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TypeList typeList = (TypeList) o;
    return Objects.equals(types, typeList.types);
  }

  @Override
  public int hashCode() {
    return Objects.hash(types);
  }
}
