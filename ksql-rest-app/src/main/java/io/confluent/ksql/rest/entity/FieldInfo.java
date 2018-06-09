/**
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
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class FieldInfo {
  private final String name;
  private final SchemaInfo schema;

  @JsonCreator
  public FieldInfo(
      @JsonProperty("name") final String name,
      @JsonProperty("schema") final SchemaInfo schema) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(schema);
    this.name = name;
    this.schema = schema;
  }


  public String getName() {
    return this.name;
  }

  public SchemaInfo getSchema() {
    return schema;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof FieldInfo
        && Objects.equals(name, ((FieldInfo)other).name)
        && Objects.equals(schema, ((FieldInfo)other).schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, schema);
  }
}
