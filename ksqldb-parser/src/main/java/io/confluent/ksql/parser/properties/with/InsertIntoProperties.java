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

package io.confluent.ksql.parser.properties.with;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.properties.with.InsertIntoConfigs;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Performs validation of a INSERT statement's WITH clause.
 */
@Immutable
public final class InsertIntoProperties {
  public static InsertIntoProperties none() {
    return new InsertIntoProperties(Collections.emptyMap());
  }

  public static InsertIntoProperties from(final Map<String, Literal> literals) {
    return new InsertIntoProperties(literals);
  }

  private final PropertiesConfig props;

  private InsertIntoProperties(final Map<String, Literal> originals) {
    this.props = new PropertiesConfig(InsertIntoConfigs.CONFIG_METADATA, originals);
  }

  public Optional<String> getQueryId() {
    return Optional.ofNullable(props.getString(InsertIntoConfigs.QUERY_ID_PROPERTY))
        .map(String::toUpperCase);
  }

  @Override
  public String toString() {
    return props.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final InsertIntoProperties that = (InsertIntoProperties) o;
    return Objects.equals(props, that.props);
  }

  @Override
  public int hashCode() {
    return Objects.hash(props);
  }
}
