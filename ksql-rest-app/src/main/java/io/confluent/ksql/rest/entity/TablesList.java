package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.confluent.ksql.metastore.KSQLTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonTypeName("tables")
public class TablesList extends KSQLEntity {
  private final List<KSQLTable> tables;

  public TablesList(String statementText, List<KSQLTable> tables) {
    super(statementText);
    this.tables = tables;
  }

  @JsonUnwrapped
  public List<KSQLTable> getTables() {
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
}
