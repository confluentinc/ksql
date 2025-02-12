/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
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

import static io.confluent.ksql.util.ParserUtil.getLocation;
import static io.confluent.ksql.util.ParserUtil.isQuoted;
import static io.confluent.ksql.util.ParserUtil.sanitize;
import static io.confluent.ksql.util.ParserUtil.unquote;
import static java.util.Objects.requireNonNull;

import io.confluent.ksql.parser.exception.ParseFailedException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.text.StringSubstitutor;

public final class VariableSubstitutor {
  private VariableSubstitutor() {
  }

  private static final Pattern VALID_IDENTIFIER_NAMES = Pattern.compile("[A-Za-z_][A-Za-z0-9_@]*");
  public static final String PREFIX = "${";
  public static final String SUFFIX = "}";

  static class VariablesLookup {
    public static Set<String> lookup(final String text) {
      final Set<String> variables = new HashSet<>();

      // Used only to lookup for variables
      final StringSubstitutor substr = new StringSubstitutor(key -> {
        variables.add(key);
        return null;
      });

      substr.setVariablePrefix(PREFIX);
      substr.setVariableSuffix(SUFFIX);
      substr.replace(text); // Nothing is replaced. It just lookups for variables.

      return variables;
    }
  }

  public static String substitute(
      final String string,
      final Map<String, String> valueMap
  ) {
    return StringSubstitutor.replace(
        string, valueMap.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey(), e -> sanitize(e.getValue())))
    );
  }

  public static String substitute(
      final KsqlParser.ParsedStatement parsedStatement,
      final Map<String, String> valueMap
  ) {
    final String statementText = parsedStatement.getUnMaskedStatementText();
    final SqlSubstitutorVisitor visitor = new SqlSubstitutorVisitor(statementText, valueMap);
    return visitor.replace(parsedStatement.getStatement());
  }

  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private static final class SqlSubstitutorVisitor extends SqlBaseBaseVisitor<Void> {
    // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

    private final String statementText;
    private final Map<String, String> valueMap;

    // Contains sanitized values for variable substitution
    private Map<String, String> sanitizedValueMap;

    SqlSubstitutorVisitor(final String statementText, final Map<String, String> valueMap) {
      this.statementText = requireNonNull(statementText, "statementText");
      this.valueMap = requireNonNull(valueMap, "valueMap");
      this.sanitizedValueMap = new HashMap<>(valueMap.size());
    }

    public String replace(final SqlBaseParser.SingleStatementContext singleStatementContext) {
      // walk the statement tree to validate and sanitize variables
      visit(singleStatementContext);

      // replace all variables with sanitized values
      return StringSubstitutor.replace(statementText, sanitizedValueMap);
    }

    private void lookupVariables(final String text) {
      for (String variableName : VariablesLookup.lookup(text)) {
        if (valueMap.containsKey(variableName)) {
          sanitizedValueMap.putIfAbsent(variableName, sanitize(valueMap.get(variableName)));
        }
      }
    }

    @Override
    public Void visitResourceName(final SqlBaseParser.ResourceNameContext context) {
      if (context.STRING() != null) {
        final String text = unquote(context.STRING().getText(), "'");
        lookupVariables(text);
      } else {
        visit(context.identifier());
      }
      return null;
    }

    @Override
    public Void visitStringLiteral(final SqlBaseParser.StringLiteralContext context) {
      final String text = unquote(context.getText(), "\'");
      lookupVariables(text);

      return null;
    }

    @Override
    public Void visitVariableLiteral(final SqlBaseParser.VariableLiteralContext context) {
      final String variableRef = context.getText();
      final String variableName = unwrap(variableRef);
      final String variableValue = valueMap.getOrDefault(variableName, variableRef);

      throwIfInvalidLiteral(variableValue, getLocation(context));
      sanitizedValueMap.putIfAbsent(variableName, sanitize(variableValue));
      return null;
    }

    @Override
    public Void visitVariableValue(final SqlBaseParser.VariableValueContext context) {
      final String text = unquote(context.getText(), "\'");
      lookupVariables(text);

      return null;
    }

    @Override
    public Void visitVariableIdentifier(final SqlBaseParser.VariableIdentifierContext context) {
      final String variableRef = context.getText();
      final String variableName = unwrap(variableRef);
      final String variableValue = valueMap.getOrDefault(variableName, variableRef);

      throwIfInvalidIdentifier(variableValue, getLocation(context));
      sanitizedValueMap.putIfAbsent(variableName, variableValue);
      return null;
    }

    @Override
    public Void visitSetProperty(final SqlBaseParser.SetPropertyContext context) {
      lookupVariables(context.STRING(0).getText());
      lookupVariables(context.STRING(1).getText());
      return null;
    }

    @Override
    public Void visitUnsetProperty(final SqlBaseParser.UnsetPropertyContext context) {
      lookupVariables(context.STRING().getText());
      return null;
    }

    private String getIdentifierText(final String value) {
      final char firstChar = value.charAt(0);
      final char lastChar = value.charAt(value.length() - 1);

      if (firstChar == '"' && lastChar == '"') {
        return unquote(value, "\"");
      } else if (firstChar == '`' && lastChar == '`') {
        return unquote(value, "`");
      } else {
        return value;
      }
    }

    static String unwrap(final String value) {
      return value.substring(PREFIX.length(), value.length() - SUFFIX.length());
    }

    private void throwIfInvalidIdentifier(
        final String value,
        final Optional<NodeLocation> location
    ) {
      final String identifierText = getIdentifierText(value);

      if (!VALID_IDENTIFIER_NAMES.matcher(identifierText).matches()) {
        throw new ParseFailedException(
            "Illegal argument at " + location.map(NodeLocation::toString).orElse("?")
                + ". Identifier names cannot start with '@' and may only contain alphanumeric "
                + "values and '_'.",
            "Illegal argument at " + location.map(NodeLocation::toString).orElse("?")
                + ". Identifier names cannot start with '@' and may only contain alphanumeric "
                + "values and '_'. Got: '" + value + "'",
            statementText);
      }
    }

    private void throwIfInvalidLiteral(
        final String value,
        final Optional<NodeLocation> location
    ) {
      // Strings are quoted
      if (isQuoted(value, "'")) {
        return;
      }

      // Booleans are unquoted
      if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
        return;
      }

      // Numbers are unquoted
      if (isNumber(value)) {
        return;
      }

      throw new ParseFailedException(
          "Illegal argument at " + location.map(NodeLocation::toString).orElse("?") + ".",
          "Illegal argument at " + location.map(NodeLocation::toString).orElse("?")
              + ". Got: '" + value + "'",
          statementText);
    }

    @SuppressWarnings({"NPathComplexity", "CyclomaticComplexity", "BooleanExpressionComplexity"})
    private boolean isNumber(final String value) {
      try {
        Long.parseLong(value);
        return true;
      } catch (final NumberFormatException e) {
        // ignored. move to the next check
      }

      try {
        final double d = Double.parseDouble(value);
        if (!Double.isInfinite(d) && !Double.isNaN(d)) {
          return true;
        }
      } catch (final NumberFormatException e) {
        // ignored. move to the next check
      }

      try {
        new BigDecimal(value);
        return true;
      } catch (final NumberFormatException e) {
        // ignored. move to the next check
      }

      return false;
    }
  }
}
