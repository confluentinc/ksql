/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server;

import io.confluent.ksql.KQLEngine;
import io.confluent.ksql.parser.tree.Statement;

import java.util.List;

public class StatementParser {
  private final KQLEngine kqlEngine;

  public StatementParser(KQLEngine kqlEngine) {
    this.kqlEngine = kqlEngine;
  }

  public Statement parseSingleStatement(String statementString) throws Exception {
    List<Statement> statements = kqlEngine.getStatements(statementString);
    if (statements == null) {
      throw new IllegalArgumentException("Call to KQLEngine.getStatements() returned null");
    } else if ((statements.size() != 1)) {
      throw new IllegalArgumentException(
          String.format("Expected exactly one KQL statement; found %d instead", statements.size())
      );
    } else {
      return statements.get(0);
    }
  }
}
