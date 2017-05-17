package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.confluent.ksql.metastore.KSQLTable;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

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
    private final Schema schema;
    private final Field keyField;
    private final TopicsList.TopicInfo topicInfo;
    private final String stateStoreName;
    private final boolean isWindowed;

    @JsonCreator
    public TableInfo(
        @JsonProperty("name")           String name,
        @JsonProperty("schema")         Schema schema,
        @JsonProperty("keyField")       Field keyField,
        @JsonProperty("topicInfo")      TopicsList.TopicInfo topicInfo,
        @JsonProperty("stateStoreName") String stateStoreName,
        @JsonProperty("isWindowed")     boolean isWindowed
    ) {
      this.name = name;
      this.schema = schema;
      this.keyField = keyField;
      this.topicInfo = topicInfo;
      this.stateStoreName = stateStoreName;
      this.isWindowed = isWindowed;
    }

    public TableInfo(KSQLTable ksqlTable) {
      this(
          ksqlTable.getName(),
          ksqlTable.getSchema(),
          ksqlTable.getKeyField(),
          new TopicsList.TopicInfo(ksqlTable.getKsqlTopic()),
          ksqlTable.getStateStoreName(),
          ksqlTable.isWinidowed()
      );
    }

    public String getName() {
      return name;
    }

    public Schema getSchema() {
      return schema;
    }

    public Field getKeyField() {
      return keyField;
    }

    public TopicsList.TopicInfo getTopicInfo() {
      return topicInfo;
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
          Objects.equals(getSchema(), tableInfo.getSchema()) &&
          Objects.equals(getKeyField(), tableInfo.getKeyField()) &&
          Objects.equals(getTopicInfo(), tableInfo.getTopicInfo()) &&
          Objects.equals(getStateStoreName(), tableInfo.getStateStoreName());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getSchema(), getKeyField(), getTopicInfo(), getStateStoreName(), getIsWindowed());
    }
  }
}
