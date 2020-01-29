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

import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Searches through the AST for any column references and throws if they are unknown or ambiguous.
 */
class ExpressionAnalyzer {

  private final SourceSchemas sourceSchemas;

  ExpressionAnalyzer(final SourceSchemas sourceSchemas) {
    this.sourceSchemas = Objects.requireNonNull(sourceSchemas, "sourceSchemas");
  }

  Set<SourceName> analyzeExpression(final Expression expression) {
    final Set<SourceName> referencedSources = new HashSet<>();
    final SourceExtractor extractor = new SourceExtractor(referencedSources);
    extractor.process(expression, null);
    return referencedSources;
  }

  private final class SourceExtractor extends TraversalExpressionVisitor<Object> {

    private final Set<SourceName> referencedSources;

    SourceExtractor(final Set<SourceName> referencedSources) {
      this.referencedSources = Objects.requireNonNull(referencedSources, "referencedSources");
    }

    @Override
    public Void visitColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Object context
    ) {
      final ColumnRef reference = node.getReference();
      getSource(Optional.empty(), reference).ifPresent(referencedSources::add);
      return null;
    }

    @Override
    public Void visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Object context
    ) {
      getSource(Optional.of(node.getQualifier()), node.getReference())
          .ifPresent(referencedSources::add);
      return null;
    }

    private Optional<SourceName> getSource(
        final Optional<SourceName> sourceName,
        final ColumnRef name
    ) {
      final Set<SourceName> sourcesWithField = sourceSchemas.sourcesWithField(sourceName, name);
      if (sourcesWithField.isEmpty()) {
        throw new KsqlException("Column '"
            + sourceName.map(n -> n.name() + KsqlConstants.DOT + name.name().name())
                .orElse(name.name().name())
            + "' cannot be resolved.");
      }

      if (sourcesWithField.size() > 1) {
        final String possibilities = sourcesWithField.stream()
            .map(source -> SchemaUtil.buildAliasedFieldName(source.name(), name.name().name()))
            .sorted()
            .collect(Collectors.joining(", "));

        throw new KsqlException("Column '" + name.name().name() + "' is ambiguous. "
            + "Could be any of: " + possibilities);
      }

      return Optional.of(Iterables.getOnlyElement(sourcesWithField));
    }
  }
}
