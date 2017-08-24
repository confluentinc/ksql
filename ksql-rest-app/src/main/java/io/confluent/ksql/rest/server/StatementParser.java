/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
