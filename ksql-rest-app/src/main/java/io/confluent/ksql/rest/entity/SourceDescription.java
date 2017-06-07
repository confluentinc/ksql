/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.StructuredDataSource;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.Objects;
import java.util.Optional;

@JsonTypeName("description")
public class SourceDescription extends KsqlEntity {

  private final String name;
  private final Schema schema;
  private final DataSource.DataSourceType type;
  private final String key;
  private final String timestamp;

  @JsonCreator
  public SourceDescription(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("name")          String name,
      @JsonProperty("schema")        Schema schema,
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
        dataSource.getSchema(),
        dataSource.getDataSourceType(),
        Optional.ofNullable(dataSource.getKeyField()).map(Field::name).orElse(null),
        Optional.ofNullable(dataSource.getTimestampField()).map(Field::name).orElse(null)
    );
  }

  public String getName() {
    return name;
  }

  public Schema getSchema() {
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
}
