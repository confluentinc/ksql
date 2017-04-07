/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.rest;

import io.confluent.kql.KQLEngine;
import io.confluent.kql.parser.tree.Statement;

import java.util.List;

public class StatementParser {
  private final KQLEngine kqlEngine;

  public StatementParser(KQLEngine kqlEngine) {
    this.kqlEngine = kqlEngine;
  }

  public Statement parseSingleStatement(String statementString) throws Exception {
    List<Statement> statements = kqlEngine.getStatements(statementString);
    if (statements == null) {
      throw new Exception("Call to KQLEngine.getStatements() returned null");
    } else if ((statements.size() != 1)) {
      throw new Exception(String.format("Expected exactly one KQL statement; found %d instead", statements.size()));
    } else {
      return statements.get(0);
    }
  }
}
