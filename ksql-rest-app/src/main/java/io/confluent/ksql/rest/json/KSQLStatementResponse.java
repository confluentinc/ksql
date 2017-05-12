package io.confluent.ksql.rest.json;

public abstract class KSQLStatementResponse {
  private final String statementText;

  public KSQLStatementResponse(String statementText) {
    this.statementText = statementText;
  }

  public String getStatementText() {
    return statementText;
  }
}
