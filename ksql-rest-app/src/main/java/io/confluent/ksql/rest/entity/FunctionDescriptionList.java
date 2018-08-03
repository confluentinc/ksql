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
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

public class FunctionDescriptionList extends KsqlEntity {

  private final String name;
  private final String description;
  private final String author;
  private final String version;
  private final Collection<FunctionInfo> functions;
  private final String path;
  private final FunctionType type;

  @JsonCreator
  public FunctionDescriptionList(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("name") final String name,
      @JsonProperty("description") final String description,
      @JsonProperty("author") final String author,
      @JsonProperty("version") final String version,
      @JsonProperty("path") final String path,
      @JsonProperty("functions") final Collection<FunctionInfo> functions,
      @JsonProperty("type") final FunctionType type) {
    super(statementText);
    this.name = Objects.requireNonNull(name, "name can't be null");
    this.description = Objects.requireNonNull(description, "description can't be null");
    this.author = Objects.requireNonNull(author, "author can't be null");
    this.version = Objects.requireNonNull(version, "version can't be null");
    this.path = Objects.requireNonNull(path, "path can't be null");
    this.functions = Objects.requireNonNull(functions, "functions can't be null");
    this.type = Objects.requireNonNull(type, "type can't be null");
  }

  public Collection<FunctionInfo> getFunctions() {
    return Collections.unmodifiableCollection(functions);
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getAuthor() {
    return author;
  }

  public String getVersion() {
    return version;
  }

  public String getPath() {
    return path;
  }

  public FunctionType getType() {
    return type;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final FunctionDescriptionList that = (FunctionDescriptionList) o;
    return Objects.equals(name, that.name)
        && Objects.equals(description, that.description)
        && Objects.equals(author, that.author)
        && Objects.equals(version, that.version)
        && Objects.equals(functions, that.functions)
        && Objects.equals(path, that.path)
        && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, author, version, functions, path, type);
  }

  @Override
  public String toString() {
    return "FunctionDescriptionList{"
        + "name='" + name + '\''
        + ", description='" + description + '\''
        + ", author='" + author + '\''
        + ", version='" + version + '\''
        + ", functions=" + functions
        + ", path='" + path + "'"
        + ", type='" + type.name() + "'"
        + '}';
  }
}
