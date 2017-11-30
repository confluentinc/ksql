/**
 * Copyright 2017 Confluent Inc.
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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.util.SchemaUtil;

import org.apache.kafka.connect.data.Field;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@JsonTypeName("description")
@JsonSubTypes({})
public class SourceDescription extends KsqlEntity {

  private final String name;
  private final  List<FieldSchemaInfo> schema;
  private final DataSource.DataSourceType type;
  private final String key;
  private final String timestamp;

  @JsonCreator
  public SourceDescription(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("name")          String name,
      @JsonProperty("schema")        List<FieldSchemaInfo> schema,
      @JsonProperty("type")          DataSource.DataSourceType type,
      @JsonProperty("key")           String key,
      @JsonProperty("timestamp")     String timestamp
  ) {
    super(statementText);
    this.name = name;
    this.schema = schema;
    this.type = type;
    this.key = key;
    this.timestamp = timestamp;
  }

  public SourceDescription(String statementText, StructuredDataSource dataSource) {

    this(
        statementText,
        dataSource.getName(),
        dataSource.getSchema().fields().stream().map(
            field -> {
              return new FieldSchemaInfo(field.name(), SchemaUtil
                  .getSchemaFieldName(field));
            }).collect(Collectors.toList()),
        dataSource.getDataSourceType(),
        Optional.ofNullable(dataSource.getKeyField()).map(Field::name).orElse(null),
        Optional.ofNullable(dataSource.getTimestampField()).map(Field::name).orElse(null)
    );
  }

  public String getName() {
    return name;
  }

  public List<FieldSchemaInfo> getSchema() {
    return schema;
  }

  public DataSource.DataSourceType getType() {
    return type;
  }

  public String getKey() {
    return key;
  }

  public String getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SourceDescription)) {
      return false;
    }
    SourceDescription that = (SourceDescription) o;
    return Objects.equals(getName(), that.getName())
        && Objects.equals(getSchema(), that.getSchema())
        && getType() == that.getType()
        && Objects.equals(getKey(), that.getKey())
        && Objects.equals(getTimestamp(), that.getTimestamp());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getSchema(), getType(), getKey(), getTimestamp());
  }

  public static class FieldSchemaInfo {
    private final String name;
    private final String type;

    @JsonCreator
    public FieldSchemaInfo(
        @JsonProperty("name")  String name,
        @JsonProperty("type") String type
    ) {
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FieldSchemaInfo)) {
        return false;
      }
      FieldSchemaInfo that = (FieldSchemaInfo) o;
      return Objects.equals(getName(), that.getName())
             && Objects.equals(getType(), that.getType());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getType());
    }
  }
}
