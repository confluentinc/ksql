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
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.HashSet;
import java.util.Objects;
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

  Set<ColumnRef> analyzeExpression(
      final Expression expression,
      final boolean allowWindowMetaFields
  ) {
    final Set<ColumnRef> referencedColumns = new HashSet<>();
    final ColumnExtractor extractor = new ColumnExtractor(allowWindowMetaFields, referencedColumns);
    extractor.process(expression, null);
    return referencedColumns;
  }

  private final class ColumnExtractor extends TraversalExpressionVisitor<Object> {

    private final Set<ColumnRef> referencedColumns;
    private final boolean allowWindowMetaFields;

    ColumnExtractor(
        final boolean allowWindowMetaFields,
        final Set<ColumnRef> referencedColumns
    ) {
      this.allowWindowMetaFields = allowWindowMetaFields;
      this.referencedColumns = referencedColumns;
    }

    @Override
    public Void visitColumnReference(
        final ColumnReferenceExp node,
        final Object context
    ) {
      final ColumnRef reference = node.getReference();
      referencedColumns.add(getQualifiedColumnRef(reference));
      return null;
    }

    private ColumnRef getQualifiedColumnRef(final ColumnRef name) {
      final Set<SourceName> sourcesWithField = sourceSchemas.sourcesWithField(name);

      if (sourcesWithField.isEmpty()) {
        if (allowWindowMetaFields && name.name().equals(SchemaUtil.WINDOWSTART_NAME)) {
          // window start doesn't need a qualifier as it's a special hacky column
          return name;
        }

        throw new KsqlException("Column '" + name.toString(FormatOptions.noEscape())
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

      return name.withSource(Iterables.getOnlyElement(sourcesWithField));
    }
  }
}
