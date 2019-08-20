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

package io.confluent.ksql.connect;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A model for a connector, which contains various information that
 * helps map topics to KSQL sources.
 */
@Immutable
public class Connector {

  private final String name;
  private final Predicate<String> isTopicMatch;
  private final Function<String, String> getSourceName;
  private final DataSourceType sourceType;
  private final Optional<String> keyField;

  public Connector(
      final String name,
      final Predicate<String> isTopicMatch,
      final Function<String, String> getSourceName,
      final DataSourceType sourceType,
      final String keyField) {
    this.name = Objects.requireNonNull(name, "name");
    this.isTopicMatch = Objects.requireNonNull(isTopicMatch, "isTopicMatch");
    this.getSourceName = Objects.requireNonNull(getSourceName, "getSourceName");
    this.sourceType = Objects.requireNonNull(sourceType, "sourceType");
    this.keyField = Optional.ofNullable(keyField);
  }

  public String getName() {
    return name;
  }

  public boolean matches(final String topic) {
    return isTopicMatch.test(topic);
  }

  public String mapToSource(final String topic) {
    return getSourceName.apply(topic);
  }

  public DataSourceType getSourceType() {
    return sourceType;
  }

  public Optional<String> getKeyField() {
    return keyField;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Connector that = (Connector) o;
    return Objects.equals(name, that.name)
        && sourceType == that.sourceType
        && Objects.equals(keyField, that.keyField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, sourceType, keyField);
  }

  @Override
  public String toString() {
    return "Connector{" + "name='" + name + '\''
        + ", sourceType=" + sourceType
        + ", keyField=" + keyField
        + '}';
  }
}
