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
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.SqlBaseBaseVisitor;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.SqlBaseParser.CreateConnectorContext;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.SqlBaseParser.StatementsContext;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertiesContext;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertyContext;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.lang3.StringUtils;

public class QueryMask {

  private static final ImmutableSet<String> ALLOWED_KEYS = ImmutableSet.of("connector.class");
  private static final String MASKED_VALUE = "'[string]'";

  public static String getMaskedStatement(final String query) {
    try {
      final ParseTree tree = DefaultKsqlParser.getParseTree(query);
      return new Visitor().visit(tree);
    } catch (final Exception e) {
      return maskConnectProperties(query);
    }
  }

  private static String maskConnectProperties(final String query) {
    /*
      Failed to parse statement. Mask disallowed keys by string replacing. This is a best effort
      attempt. The assumption is that properties are in right format: it matches key wrapped in
      single quotes equals value wrapped in single quotes. Key and value could be empty and there
      could be spaces around equal sign.
     */
    final StringBuffer sb = new StringBuffer();
    final Pattern pattern = Pattern.compile("('[^']+'|\"[^\"]+\")\\s*=\\s*('[^']*')",
        Pattern.DOTALL | Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
    final Matcher matcher = pattern.matcher(query);

    while (matcher.find()) {
      final String matchedString = matcher.group(0);
      final char quote = matcher.group(1).charAt(0);
      final String key = matcher.group(1).substring(1, matcher.group(1).length() - 1);

      if (!ALLOWED_KEYS.contains(key)) {
        final String replacement = quote + key + quote + "=" + MASKED_VALUE;
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
          unquotedLowerKey = key.substring(1, key.length() - 1).toLowerCase();
        }

        formattedProp.append(key);
        final String value = ALLOWED_KEYS.contains(unquotedLowerKey) ? prop.literal().getText() :
            MASKED_VALUE;
        formattedProp.append("=").append(value);
        tableProperties.add(formattedProp.toString());
      }

      masked = true;
      return String.format(" WITH (%s)", StringUtils.join(tableProperties, ", "));
    }
  }
}
