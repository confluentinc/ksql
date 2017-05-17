package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.confluent.ksql.metastore.KSQLTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonTypeName("tables")
public class TablesList extends KSQLEntity {
  private final Collection<TableInfo> tables;

  @JsonCreator
  public TablesList(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("tables")        Collection<TableInfo> tables
  ) {
    super(statementText);
    this.tables = tables;
  }

  public static TablesList fromKsqlTables(String statementText, Collection<KSQLTable> ksqlTables) {
    Collection<TableInfo> tableInfos = ksqlTables.stream().map(TableInfo::new).collect(Collectors.toList());
    return new TablesList(statementText, tableInfos);
  }

  @JsonUnwrapped
  public List<TableInfo> getTables() {
    return new ArrayList<>(tables);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TablesList)) {
      return false;
    }
    TablesList that = (TablesList) o;
    return Objects.equals(getTables(), that.getTables());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTables());
  }

  public static class TableInfo {
    private final String name;
    private final String topic;
    private final String stateStoreName;
    private final boolean isWindowed;

    @JsonCreator
    public TableInfo(
        @JsonProperty("name")           String name,
        @JsonProperty("topic")          String topic,
        @JsonProperty("stateStoreName") String stateStoreName,
        @JsonProperty("isWindowed")     boolean isWindowed
    ) {
      this.name = name;
      this.topic = topic;
      this.stateStoreName = stateStoreName;
      this.isWindowed = isWindowed;
    }

    public TableInfo(KSQLTable ksqlTable) {
      this(
          ksqlTable.getName(),
          ksqlTable.getKsqlTopic().getName(),
          ksqlTable.getStateStoreName(),
          ksqlTable.isWinidowed()
      );
    }

    public String getName() {
      return name;
    }

    public String getTopic() {
      return topic;
    }

    public String getStateStoreName() {
      return stateStoreName;
    }

    public boolean getIsWindowed() {
      return isWindowed;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TableInfo)) {
        return false;
      }
      TableInfo tableInfo = (TableInfo) o;
      return getIsWindowed() == tableInfo.getIsWindowed() &&
          Objects.equals(getName(), tableInfo.getName()) &&
          Objects.equals(getTopic(), tableInfo.getTopic()) &&
          Objects.equals(getStateStoreName(), tableInfo.getStateStoreName());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getTopic(), getStateStoreName(), isWindowed);
    }
  }
}
