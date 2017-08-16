/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.server;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.parser.tree.Statement;

import java.util.List;

public class StatementParser {
  private final KsqlEngine ksqlEngine;

  public StatementParser(KsqlEngine ksqlEngine) {
    this.ksqlEngine = ksqlEngine;
  }

  public Statement parseSingleStatement(String statementString) throws Exception {
    List<Statement> statements = ksqlEngine.getStatements(statementString);
    if (statements == null) {
      throw new IllegalArgumentException("Call to KsqlEngine.getStatements() returned null");
    } else if ((statements.size() != 1)) {
      throw new IllegalArgumentException(
          String.format("Expected exactly one KSQL statement; found %d instead", statements.size())
      );
    } else {
      return statements.get(0);
    }
  }
}
