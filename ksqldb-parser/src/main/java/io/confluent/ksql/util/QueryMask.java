/*
 * Copyright 2021 Confluent Inc.
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 * http://www.confluent.io/confluent-community-license
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.SqlBaseBaseVisitor;
import io.confluent.ksql.parser.SqlBaseParser.CreateConnectorContext;
import io.confluent.ksql.parser.SqlBaseParser.InsertValuesContext;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.SqlBaseParser.StatementsContext;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertiesContext;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertyContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.lang3.StringUtils;

public final class QueryMask {

  private static final ImmutableSet<String> ALLOWED_KEYS = ImmutableSet.of("connector.class");
  private static final String MASKED_STRING = "'[string]'";
  private static final String MASKED_VALUE = "'[value]'";

  private QueryMask() {}

  public static String getMaskedStatement(final String query) {
    try {
      final ParseTree tree = DefaultKsqlParser.getParseTree(query);
      return new Visitor().visit(tree);
    } catch (final Exception | StackOverflowError e) {
      return fallbackMasking(query);
    }
  }

  public static Map<String, String> getMaskedConnectConfig(final Map<String, String> config) {
    return config.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> {
      if (ALLOWED_KEYS.contains(e.getKey())) {
        return e.getValue();
      }
      return MASKED_STRING;
    }));
  }

  private static String fallbackMasking(final String query) {
    return fallbackMaskConnectProperties(fallbackMaskValues(query));
  }

  private static String fallbackMaskValues(final String query) {
    /*
       Failed to parse statement. Mask Insert values by string replacement in a best effort manner.
       The assumption is that parenthesis around VALUES are not missing.
       Regex matches sequences of characters that are not in '(' ',' ';' ')' and are not be followed
       by a '(' anywhere further down the string (since VALUES is always last in a statement).
     */
    if (StringUtils.indexOfIgnoreCase(query.replaceAll("\\s+", ""), ")VALUES(") < 0) {
      return query;
    }
    final int valueIdx = StringUtils.indexOfIgnoreCase(query, "VALUES");

    final StringBuffer sb = new StringBuffer();
    final Pattern pattern = Pattern.compile("([^(,;)]+)(?!.*\\()",
        Pattern.DOTALL | Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

    final Matcher matcher = pattern.matcher(query);
    while (matcher.find()) {
      if (matcher.start() > valueIdx) {
        matcher.appendReplacement(sb, MASKED_VALUE);
      }
    }

    matcher.appendTail(sb);
    return sb.toString();
  }

  private static String fallbackMaskConnectProperties(final String query) {
    /*
      Failed to parse statement. Mask disallowed keys by string replacing. This is a best effort
      attempt. The assumption is that properties are in right format: it matches key wrapped in
      single/double/back quotes equals value wrapped in single quotes. Value could be
      empty and there could be spaces around equal sign.
     */
    final StringBuffer sb = new StringBuffer();
    final Pattern pattern = Pattern.compile("('[^']+'|\"[^\"]+\"|`[^`]+`)\\s*=\\s*('[^']*')",
        Pattern.DOTALL | Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
    final Matcher matcher = pattern.matcher(query);

    while (matcher.find()) {
      final String matchedString = matcher.group(0);
      final char quote = matcher.group(1).charAt(0);
      final String key = matcher.group(1).substring(1, matcher.group(1).length() - 1);

      if (!ALLOWED_KEYS.contains(key)) {
        final String replacement = quote + key + quote + "=" + MASKED_STRING;
        matcher.appendReplacement(sb, replacement);
      } else {
        matcher.appendReplacement(sb, matchedString);
      }
    }

    matcher.appendTail(sb);
    return sb.toString();
  }

  private static final class Visitor extends SqlBaseBaseVisitor<String> {

    private boolean masked;

    @Override
    public String visitStatements(final StatementsContext context) {
      final List<String> statementList = new ArrayList<>();
      for (final SingleStatementContext stmtContext : context.singleStatement()) {

        masked = false;
        final String statement = visitSingleStatement(stmtContext);

        if (masked) {
          statementList.add(statement);
        } else {
          // Use original statement if not masked
          final int start = stmtContext.start.getStartIndex();
          final int end = stmtContext.stop.getStopIndex();
          final Interval interval = new Interval(start, end);
          final String original = stmtContext.start.getInputStream().getText(interval);
          statementList.add(original);
        }
      }
      return StringUtils.join(statementList, "\n");
    }

    @Override
    public String visitSingleStatement(final SingleStatementContext context) {
      return String.format("%s;", visit(context.statement()));
    }

    @Override
    public String visitInsertValues(final InsertValuesContext context) {
      final StringBuilder stringBuilder = new StringBuilder("INSERT INTO ");

      if (context.sourceName() != null) {
        stringBuilder.append(ParserUtil.getSourceName(context.sourceName()));
      }

      // columns
      if (context.columns() != null) {
        stringBuilder.append(String.format(" (%s)",
            StringUtils.join(context.columns().identifier().stream()
                .map(ParserUtil::getIdentifierText)
                .map(ColumnName::of)
                .map(ColumnName::toString).collect(Collectors.toList()), ", ")));
      }

      // masked values
      final String values = String.join(", ",
          Collections.nCopies(context.values().valueExpression().size(), MASKED_VALUE));
      stringBuilder.append(String.format(" VALUES (%s)", values));

      masked = true;
      return stringBuilder.toString();
    }

    @Override
    public String visitCreateConnector(final CreateConnectorContext context) {
      final StringBuilder stringBuilder = new StringBuilder("CREATE");

      if (context.SOURCE() != null) {
        stringBuilder.append(" SOURCE");
      } else if (context.SINK() != null) {
        stringBuilder.append(" SINK");
      }

      stringBuilder.append(" CONNECTOR");

      // optional if not exists
      if (context.EXISTS() != null) {
        stringBuilder.append(" IF NOT EXISTS");
      }

      stringBuilder.append(" ").append(context.identifier().getText());


      // mask properties
      if (context.tableProperties() != null) {
        stringBuilder.append(visit(context.tableProperties()));
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitTableProperties(final TablePropertiesContext context) {
      if (!(context.parent instanceof CreateConnectorContext)) {
        return null;
      }
      final List<String> tableProperties = new ArrayList<>();
      // Mask everything except "connector.class"
      for (final TablePropertyContext prop : context.tableProperty()) {
        final StringBuilder formattedProp = new StringBuilder();
        final String unquotedLowerKey;
        final String key;

        if (prop.identifier() != null) {
          key = prop.identifier().getText();
          unquotedLowerKey = ParserUtil.getIdentifierText(prop.identifier()).toLowerCase();
        } else {
          key = prop.STRING().getText();
          unquotedLowerKey = ParserUtil.unquote(key, "'");
        }

        formattedProp.append(key);
        final String value = ALLOWED_KEYS.contains(unquotedLowerKey) ? prop.literal().getText() :
            MASKED_STRING;
        formattedProp.append("=").append(value);
        tableProperties.add(formattedProp.toString());
      }

      masked = true;
      return String.format(" WITH (%s)", StringUtils.join(tableProperties, ", "));
    }
  }
}
