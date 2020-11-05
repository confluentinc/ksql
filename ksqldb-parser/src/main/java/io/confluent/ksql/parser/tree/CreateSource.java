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

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import java.util.Objects;
import java.util.Optional;

@Immutable
public abstract class CreateSource extends Statement {

  private final SourceName name;
  private final TableElements elements;
  private final boolean notExists;
  private final CreateSourceProperties properties;
  private final boolean orReplace;

  CreateSource(
      final Optional<NodeLocation> location,
      final SourceName name,
      final TableElements elements,
      final boolean orReplace,
      final boolean notExists,
      final CreateSourceProperties properties
  ) {
    super(location);
    this.name = requireNonNull(name, "name");
    this.elements = requireNonNull(elements, "elements");
    this.orReplace = orReplace;
    this.notExists = notExists;
    this.properties = requireNonNull(properties, "properties");
  }

  public CreateSourceProperties getProperties() {
    return properties;
  }

  public SourceName getName() {
    return name;
  }

  public TableElements getElements() {
    return elements;
  }

  public boolean isOrReplace() {
    return orReplace;
  }

  public boolean isNotExists() {
    return notExists;
  }

  public abstract CreateSource copyWith(TableElements elements, CreateSourceProperties properties);

  @Override
  public int hashCode() {
    return Objects.hash(name, elements, orReplace, notExists, properties);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateSource)) {
      return false;
    }
    final CreateSource that = (CreateSource) o;
    return notExists == that.notExists
        && orReplace == that.orReplace
        && Objects.equals(name, that.name)
        && Objects.equals(elements, that.elements)
        && Objects.equals(properties, that.properties);
  }
}
