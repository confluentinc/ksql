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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.FieldInfo;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.SourceDescription;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class SourceDescriptionImpl implements SourceDescription {

  private final String name;
  private final String type;
  private final List<FieldInfo> fields;
  private final String topic;
  private final String keyFormat;
  private final String valueFormat;
  private final List<QueryInfo> readQueries;
  private final List<QueryInfo> writeQueries;
  private final Optional<String> timestampColumn;
  private final Optional<String> windowType;
  private final String sqlStatement;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  SourceDescriptionImpl(
      final String name,
      final String type,
      final List<FieldInfo> fields,
      final String topic,
      final String keyFormat,
      final String valueFormat,
      final List<QueryInfo> readQueries,
      final List<QueryInfo> writeQueries,
      final Optional<String> timestampColumn,
      final Optional<String> windowType,
      final String sqlStatement
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    this.name = Objects.requireNonNull(name, "name");
    this.type = Objects.requireNonNull(type, "type");
    this.fields = Objects.requireNonNull(fields, "fields");
    this.topic = Objects.requireNonNull(topic, "topic");
    this.keyFormat = Objects.requireNonNull(keyFormat, "keyFormat");
    this.valueFormat = Objects.requireNonNull(valueFormat, "valueFormat");
    this.readQueries = Objects.requireNonNull(readQueries, "readQueries");
    this.writeQueries = Objects.requireNonNull(writeQueries, "writeQueries");
    this.timestampColumn = Objects.requireNonNull(timestampColumn, "timestampColumn");
    this.windowType = Objects.requireNonNull(windowType, "windowType");
    this.sqlStatement = Objects.requireNonNull(sqlStatement, "sqlStatement");
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String type() {
    return type;
  }

  @Override
  public List<FieldInfo> fields() {
    return fields;
  }

  @Override
  public String topic() {
    return topic;
  }

  @Override
  public String keyFormat() {
    return keyFormat;
  }

  @Override
  public String valueFormat() {
    return valueFormat;
  }

  @Override
  public List<QueryInfo> readQueries() {
    return readQueries;
  }

  @Override
  public List<QueryInfo> writeQueries() {
    return writeQueries;
  }

  @Override
  public Optional<String> timestampColumn() {
    return timestampColumn;
  }

  @Override
  public Optional<String> windowType() {
    return windowType;
  }

  @Override
  public String sqlStatement() {
    return sqlStatement;
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  @Override
  public boolean equals(final Object o) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SourceDescriptionImpl that = (SourceDescriptionImpl) o;
    return name.equals(that.name)
        && type.equals(that.type)
        && fields.equals(that.fields)
        && topic.equals(that.topic)
        && keyFormat.equals(that.keyFormat)
        && valueFormat.equals(that.valueFormat)
        && readQueries.equals(that.readQueries)
        && writeQueries.equals(that.writeQueries)
        && timestampColumn.equals(that.timestampColumn)
        && windowType.equals(that.windowType)
        && sqlStatement.equals(that.sqlStatement);
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(name, type, fields, topic, keyFormat, valueFormat, readQueries,
            writeQueries, timestampColumn, windowType, sqlStatement);
  }

  @Override
  public String toString() {
    return "SourceDescription{"
        + "name='" + name + '\''
        + ", type='" + type + '\''
        + ", fields=" + fields
        + ", topic='" + topic + '\''
        + ", keyFormat='" + keyFormat + '\''
        + ", valueFormat='" + valueFormat + '\''
        + ", readQueries=" + readQueries
        + ", writeQueries=" + writeQueries
        + ", timestampColumn='" + timestampColumn + '\''
        + ", windowType=" + windowType
        + ", sqlStatement='" + sqlStatement + '\''
        + '}';
  }
}
