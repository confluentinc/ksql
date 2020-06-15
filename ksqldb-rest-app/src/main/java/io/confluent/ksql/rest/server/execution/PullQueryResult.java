package io.confluent.ksql.rest.server.execution;

import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.rest.entity.TableRowsEntity;

public class PullQueryResult {

  private final TableRowsEntity tableRowsEntity;
  private final KsqlNode node;

  public PullQueryResult(
      final TableRowsEntity tableRowsEntity,
      final KsqlNode node) {

    this.tableRowsEntity = tableRowsEntity;
    this.node = node;
  }

  public TableRowsEntity getTableRowsEntity() {
    return tableRowsEntity;
  }

  public KsqlNode getNode() {
    return node;
  }
}
