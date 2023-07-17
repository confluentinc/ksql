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

package io.confluent.ksql.function.udf;

import java.util.Objects;

public class UdfMetadata {
  private final String name;
  private final String description;
  private final String author;
  private final String version;
  private final String path;
  private final String category;

  public UdfMetadata(final String name,
                     final String description,
                     final String author,
                     final String version,
      final String category,
                     final String path
  ) {
    this.name = Objects.requireNonNull(name, "name cant be null");
    this.description = Objects.requireNonNull(description, "description can't be null");
    this.author = Objects.requireNonNull(author, "author can't be null");
    this.version = Objects.requireNonNull(version, "version can't be null");
    this.category = Objects.requireNonNull(category, "category can't be null");
    this.path = Objects.requireNonNull(path, "path can't be null");
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

  public String getCategory() {
    return category;
  }

  @Override
  public String toString() {
    return "UdfMetadata{"
        + "name='" + name + '\''
        + ", description='" + description + '\''
        + ", author='" + author + '\''
        + ", version='" + version + '\''
        + ", path='" + path + '\''
        + ", category='" + category + "'"
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final UdfMetadata that = (UdfMetadata) o;
    return Objects.equals(name, that.name)
        && Objects.equals(description, that.description)
        && Objects.equals(author, that.author)
        && Objects.equals(version, that.version)
        && Objects.equals(path, that.path)
        && Objects.equals(category, that.category);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, author, version, path, category);
  }
}
