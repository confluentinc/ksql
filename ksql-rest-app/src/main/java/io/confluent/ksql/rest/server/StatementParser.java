/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import java.util.List;

public class StatementParser {
  private final KsqlEngine ksqlEngine;

  public StatementParser(final KsqlEngine ksqlEngine) {
    this.ksqlEngine = ksqlEngine;
  }

  public Statement parseSingleStatement(final String statementString) {
    final List<PreparedStatement> statements = ksqlEngine.parseStatements(statementString);
    if ((statements.size() != 1)) {
      throw new IllegalArgumentException(
          String.format("Expected exactly one KSQL statement; found %d instead", statements.size())
      );
    } else {
      return statements.get(0).getStatement();
    }
  }
}
