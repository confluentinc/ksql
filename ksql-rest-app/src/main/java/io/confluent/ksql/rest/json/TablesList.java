package io.confluent.ksql.rest.json;

import io.confluent.ksql.metastore.KSQLTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TablesList extends KSQLStatementResponse {
  private final List<KSQLTable> tables;

  public TablesList(String statementText, List<KSQLTable> tables) {
    super(statementText);
    this.tables = tables;
  }

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
