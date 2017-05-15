package io.confluent.ksql.rest.json;

import io.confluent.ksql.metastore.DataSource;
import org.apache.kafka.connect.data.Schema;

import java.util.Objects;

public class SourceDescription extends KSQLStatementResponse {

  private final Description description;

  public SourceDescription(
      String statementText,
      String name,
      Schema schema,
      DataSource.DataSourceType type,
      String key
  ) {
    super(statementText);
    this.description = new Description(name, schema, type, key);
  }

  public Description getDescription() {
    return description;
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
    return Objects.equals(getDescription(), that.getDescription());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getDescription());
  }

  public static class Description {
    private final String name;
    private final Schema schema;
    private final DataSource.DataSourceType type;
    private final String key;

    public Description(
        String name,
        Schema schema,
        DataSource.DataSourceType type,
        String key
    ) {
      this.name = name;
      this.schema = schema;
      this.type = type;
      this.key = key;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Description)) {
        return false;
      }
      Description that = (Description) o;
      return Objects.equals(getName(), that.getName()) &&
          Objects.equals(getSchema(), that.getSchema()) &&
          getType() == that.getType() &&
          Objects.equals(getKey(), that.getKey());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getSchema(), getType(), getKey());
    }
  }
}
