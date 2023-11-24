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

package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class CreateConnector extends Statement {

  public enum Type {
    SOURCE,
    SINK
  }

  private final String name;
  private final ImmutableMap<String, Literal> config;
  private final Type type;
  private final boolean notExists;

  public CreateConnector(
      final Optional<NodeLocation> location,
      final String name,
      final Map<String, Literal> config,
      final Type type,
      final boolean notExists
  ) {
    super(location);
    this.name = Objects.requireNonNull(name, "name");
    this.config = ImmutableMap.copyOf(Objects.requireNonNull(config, "config"));
    this.type = Objects.requireNonNull(type, "type");
    this.notExists = notExists;

  }

  public CreateConnector(
      final String name,
      final Map<String, Literal> config,
      final Type type,
      final boolean notExists
  ) {
    this(Optional.empty(), name, config, type, notExists);
  }


  public String getName() {
    return name;
  }

  public boolean ifNotExists() {
    return notExists;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "config is ImmutableMap")
  public Map<String, Literal> getConfig() {
    return config;
  }

  public Type getType() {
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

    final CreateConnector that = (CreateConnector) o;
    return Objects.equals(name, that.name)
        && Objects.equals(config, that.config)
        && Objects.equals(notExists, that.notExists)
        && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, config, type, notExists);
  }

  @Override
  public String toString() {
    return "CreateConnector{"
        + "name='" + name + '\''
        + ", config=" + config
        + ", type=" + type
        + ", notExists" + notExists
        + '}';
  }
}
