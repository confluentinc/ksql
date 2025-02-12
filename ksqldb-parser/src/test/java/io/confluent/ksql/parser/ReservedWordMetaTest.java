/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.parser;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.commons.lang3.text.WordUtils;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * This test ensures that we don't accidentally add words to our grammar
 * without adding them to our "nonReserved" list. This is necessary because
 * adding words to our grammar is backwards incompatible if anyone has issued
 * a query in the past that used on of those words.
 */
public class ReservedWordMetaTest {

  private static final String[] RESERVED = new String[]{"SELECT", "FROM", "AS",
      "ALL", "DISTINCT", "WHERE", "WITHIN", "WINDOW", "GROUP", "BY", "HAVING",
      "LIMIT", "AT", "OR", "AND", "IN", "NOT", "EXISTS", "BETWEEN", "LIKE", "IS",
      "NULL", "TRUE", "FALSE", "MILLISECOND", "YEARS", "MONTHS", "DAYS", "HOURS",
      "MINUTES", "SECONDS", "MILLISECONDS", "TUMBLING", "HOPPING", "SIZE", "ADVANCE",
      "RETENTION", "CASE", "WHEN", "THEN", "ELSE", "END", "JOIN", "FULL", "OUTER",
      "INNER", "LEFT", "RIGHT", "ON", "WITH", "VALUES", "CREATE", "TABLE", "TOPIC",
      "STREAM", "STREAMS", "INSERT", "DELETE", "INTO", "DESCRIBE", "EXTENDED",
      "PRINT", "CAST", "LIST", "TOPICS", "QUERY", "QUERIES", "TERMINATE", "PAUSE", "RESUME",
      "LOAD", "DROP", "TO", "RENAME", "SAMPLE", "EXPORT", "CATALOG", "PROPERTIES",
      "BEGINNING", "UNSET", "RUN", "SCRIPT", "DECIMAL", "CONNECTOR", "CONNECTORS",
      "NAMESPACE", "MATERIALIZED", "VIEW", "EQ", "NEQ", "LT", "LTE", "GT", "GTE",
      "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "CONCAT", "ASSIGN",
      "STRUCT_FIELD_REF", "STRING", "INTEGER_VALUE", "DECIMAL_VALUE",
      "FLOATING_POINT_VALUE", "IDENTIFIER", "DIGIT_IDENTIFIER", "QUOTED_IDENTIFIER",
      "BACKQUOTED_IDENTIFIER", "TIME_WITH_TIME_ZONE", "TIMESTAMP_WITH_TIME_ZONE",
      "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED", "VARIABLE",
      "DIRECTIVE_COMMENT", "LAMBDA_EXPRESSION"};
  private static final Set<String> RESERVED_SET = ImmutableSet.copyOf(RESERVED);

  /**
   * Enable this test to re-generate the grammar that is intentionally
   * part of the reserved words.
   */
  @Test
  @Ignore
  public void generateExcludedWords() {
    final Vocabulary vocabulary = SqlBaseParser.VOCABULARY;
    final int tokens = vocabulary.getMaxTokenType();
    final ImmutableSet.Builder<String> builder = ImmutableSet.builder();

    for (int i = 0; i < tokens; i++) {
      final String symbolicName = vocabulary.getSymbolicName(i);
      if (symbolicName != null) {
        try {
          buildParser(symbolicName).nonReserved();
        } catch (final ParsingException e) {
          // word is reserved (not in nonReserved)
          builder.add(symbolicName);
        }
      }
    }

    final ImmutableSet<String> reserved = builder.build();
    System.out.println(WordUtils.wrap("private static final String[] RESERVED = new String[]{"
        + reserved
        .stream()
        .map(s -> '"' + s + '"')
        .collect(Collectors.joining(", ")) + "};", 80, "\n\t\t", false));
  }

  @Test
  public void shouldAddWordToNonReserved() {
    final Vocabulary vocabulary = SqlBaseParser.VOCABULARY;
    final int tokens = vocabulary.getMaxTokenType();

    for (int i = 0; i < tokens; i++) {
      final String symbolicName = vocabulary.getSymbolicName(i);
      if (symbolicName != null) {
        try {
          buildParser(symbolicName).nonReserved();
        } catch (final ParsingException e) {
          assertThat(
              String.format("Detected new word ('%s') in the vocabulary that is reserved. "
                  + "This is a breaking change!! If this is intentional, then add it to "
                  + "the RESERVED set in this test, otherwise please add it to the nonReserved "
                  + "parser rule.", symbolicName), symbolicName, Matchers.isIn(RESERVED_SET));
        }
      }
    }
  }

  private static SqlBaseParser buildParser(final String symbolicName) {
    final SqlBaseLexer lexer = new SqlBaseLexer(
        new CaseInsensitiveStream(CharStreams.fromString(symbolicName)));

    final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    final SqlBaseParser parser = new SqlBaseParser(tokenStream);
    parser.removeErrorListeners();
    parser.addErrorListener(DefaultKsqlParser.ERROR_VALIDATOR);
    parser.getInterpreter().setPredictionMode(PredictionMode.LL);

    return parser;
  }

}
