/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */


package io.confluent.ksql.util;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class GrammarTokenExporterTest {
  private static final List<String> expectedTokens = new ArrayList<>(Arrays.asList("EMIT", "CHANGES",
      "FINAL", "SELECT", "FROM", "AS", "ALL", "DISTINCT", "WHERE", "WITHIN",
      "WINDOW", "GROUP", "BY", "HAVING", "LIMIT", "AT", "OR", "AND", "IN", "NOT",
      "EXISTS", "BETWEEN", "LIKE", "ESCAPE", "IS", "NULL", "TRUE", "FALSE",
      "INTEGER", "DATE", "TIME", "TIMESTAMP", "INTERVAL", "YEAR", "MONTH", "DAY",
      "HOUR", "MINUTE", "SECOND", "MILLISECOND", "YEARS", "MONTHS", "DAYS",
      "HOURS", "MINUTES", "SECONDS", "MILLISECONDS", "ZONE", "TUMBLING", "HOPPING",
      "SIZE", "ADVANCE", "RETENTION", "GRACE", "PERIOD", "CASE", "WHEN", "THEN",
      "ELSE", "END", "JOIN", "FULL", "OUTER", "INNER", "LEFT", "RIGHT", "ON",
      "PARTITION", "STRUCT", "WITH", "VALUES", "CREATE", "TABLE", "TOPIC", "STREAM",
      "STREAMS", "INSERT", "DELETE", "INTO", "DESCRIBE", "EXTENDED", "PRINT",
      "EXPLAIN", "ANALYZE", "TYPE", "TYPES", "CAST", "SHOW", "LIST", "TABLES",
      "TOPICS", "QUERY", "QUERIES", "TERMINATE", "PAUSE", "RESUME", "LOAD", "COLUMNS", "COLUMN",
      "PARTITIONS", "FUNCTIONS", "FUNCTION", "DROP", "TO", "RENAME", "ARRAY",
      "MAP", "SET", "DEFINE", "UNDEFINE", "RESET", "SESSION", "SAMPLE", "EXPORT",
      "CATALOG", "PROPERTIES", "BEGINNING", "UNSET", "RUN", "SCRIPT", "DECIMAL",
      "KEY", "CONNECTOR", "CONNECTORS", "SINK", "SOURCE", "NAMESPACE", "MATERIALIZED",
      "VIEW", "PRIMARY", "REPLACE", "ASSERT", "ADD", "ALTER", "VARIABLES", "PLUGINS", "HEADERS",
      "HEADER", "SYSTEM", "TIMEOUT", "SCHEMA", "SUBJECT", "ID", "IF", "EQ", "NEQ", "LT",
      "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "CONCAT", "ASSIGN",
      "STRING", "IDENTIFIER", "VARIABLE", "EXPONENT", "DIGIT", "LETTER", "WS"));

  @Test
  public void shouldNeedBackQuotes() {
    List<String> tokens = GrammarTokenExporter.getTokens();

    assertEquals(expectedTokens, tokens);
  }
}
