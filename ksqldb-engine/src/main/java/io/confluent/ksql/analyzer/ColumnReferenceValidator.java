/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.analyzer;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Searches through the AST for any column references and throws if they are unknown or ambiguous.
 */
class ColumnReferenceValidator {

  private final SourceSchemas sourceSchemas;

  ColumnReferenceValidator(final SourceSchemas sourceSchemas) {
    this.sourceSchemas = requireNonNull(sourceSchemas, "sourceSchemas");
  }

  /**
   * Check the supplied expression and throw if it references any unknown columns.
   *
   * @param expression the expression to validate.
   * @param clauseType the type of clause the expression is part of, e.g. SELECT, GROUP BY, etc.
   * @return the names of the sources the expression uses.
   */
  Set<SourceName> analyzeExpression(
      final Expression expression,
      final String clauseType
  ) {
    final SourceExtractor extractor = new SourceExtractor(clauseType);
    extractor.process(expression, null);
    return extractor.referencedSources;
  }

  private final class SourceExtractor extends TraversalExpressionVisitor<Object> {

    private final String clauseType;
    private final Set<SourceName> referencedSources = new HashSet<>();

    SourceExtractor(final String clauseType) {
      this.clauseType = requireNonNull(clauseType, "clauseType");
    }

    @Override
    public Void visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Object context
    ) {
      final ColumnName reference = node.getColumnName();
      getSource(node.getLocation(), Optional.empty(), reference)
          .ifPresent(referencedSources::add);
      return null;
    }

    @Override
    public Void visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Object context
    ) {
      getSource(node.getLocation(), Optional.of(node.getQualifier()), node.getColumnName())
          .ifPresent(referencedSources::add);
      return null;
    }

    private Optional<SourceName> getSource(
        final Optional<NodeLocation> location,
        final Optional<SourceName> sourceName,
        final ColumnName name
    ) {
      final Set<SourceName> sourcesWithField = sourceSchemas.sourcesWithField(sourceName, name);
      if (sourcesWithField.isEmpty()) {
        throw new KsqlException(errorPrefix(location) + "column '"
            + sourceName.map(n -> n.text() + KsqlConstants.DOT + name.text())
            .orElse(name.text())
            + "' cannot be resolved.");
      }

      if (sourcesWithField.size() > 1) {
        final String possibilities = sourcesWithField.stream()
            .map(source -> new QualifiedColumnReferenceExp(source, name))
            .map(Objects::toString)
            .sorted()
            .collect(Collectors.joining(", "));

        throw new KsqlException(errorPrefix(location) + "column '"
            + name.text() + "' is ambiguous. "
            + "Could be any of: " + possibilities);
      }

      return Optional.of(Iterables.getOnlyElement(sourcesWithField));
    }

    private String errorPrefix(final Optional<NodeLocation> location) {
      final String loc = location
          .map(Objects::toString)
          .map(text -> text + ": ")
          .orElse("");

      return loc + clauseType + " ";
    }
  }
}
