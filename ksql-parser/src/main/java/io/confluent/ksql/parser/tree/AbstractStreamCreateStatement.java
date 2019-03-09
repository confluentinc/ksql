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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Immutable
public abstract class AbstractStreamCreateStatement extends Statement {

  private final QualifiedName name;
  private final ImmutableList<TableElement> elements;
  private final boolean notExists;
  private final ImmutableMap<String, Expression> properties;

  AbstractStreamCreateStatement(
      final Optional<NodeLocation> location,
      final QualifiedName name,
      final List<TableElement> elements,
      final boolean notExists,
      final Map<String, Expression> properties
  ) {
    super(location);
    this.name = requireNonNull(name, "name");
    this.elements = ImmutableList.copyOf(requireNonNull(elements, "elements"));
    this.notExists = notExists;
    this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties"));
  }

  public Map<String, Expression> getProperties() {
    return properties;
  }

  public QualifiedName getName() {
    return name;
  }

  public List<TableElement> getElements() {
    return elements;
  }

  public boolean isNotExists() {
    return notExists;
  }

  public abstract AbstractStreamCreateStatement copyWith(
      List<TableElement> elements,
      Map<String, Expression> properties);

  @Override
  public int hashCode() {
    return Objects.hash(name, elements, notExists, properties);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractStreamCreateStatement)) {
      return false;
    }
    final AbstractStreamCreateStatement that = (AbstractStreamCreateStatement) o;
    return notExists == that.notExists
        && Objects.equals(name, that.name)
        && Objects.equals(elements, that.elements)
        && Objects.equals(properties, that.properties);
  }
}
