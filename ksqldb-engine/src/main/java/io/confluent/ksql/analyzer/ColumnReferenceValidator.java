/*
 * Copyright 2021 Confluent Inc.
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

import static io.confluent.ksql.schema.ksql.SystemColumns.isWindowBound;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.util.UnknownColumnException;
import java.util.HashSet;
import java.util.Set;

/**
 * Searches through the AST for any column references and throws if they are unknown or ambiguous.
 */
class ColumnReferenceValidator {

  private final SourceSchemas sourceSchemas;
  private final boolean possibleSyntheticColumns;

  ColumnReferenceValidator(
      final SourceSchemas sourceSchemas,
      final boolean possibleSyntheticColumns
  ) {
    this.sourceSchemas = requireNonNull(sourceSchemas, "sourceSchemas");
    this.possibleSyntheticColumns = possibleSyntheticColumns;
  }

  /**
   * Check the supplied expression and throw if it references any unknown columns.
   *
   * @param expression the expression to validate.
   * @return the names of the sources the expression uses.
   */
  Set<SourceName> analyzeExpression(
      final Expression expression,
      final String clauseType
  ) {
    final Validator extractor = new Validator(clauseType);
    extractor.process(expression, null);
    return extractor.referencedSources;
  }

  private final class Validator extends TraversalExpressionVisitor<Object> {

    private final String clauseType;
    private final Set<SourceName> referencedSources = new HashSet<>();

    Validator(final String clauseType) {
      this.clauseType = requireNonNull(clauseType, "clauseType");
    }

    @Override
    public Void visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Object context
    ) {
      validateColumn(node);
      return null;
    }

    @Override
    public Void visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Object context
    ) {
      validateColumn(node);
      return null;
    }

    private void validateColumn(final ColumnReferenceExp colRef) {
      final Set<SourceName> sourcesWithField = sourceSchemas
          .sourcesWithField(colRef.maybeQualifier(), colRef.getColumnName());

      if (sourcesWithField.isEmpty()) {
        if (couldBeSyntheticJoinColumn(colRef)) {
          // Validating this is handled in the logical model.
          return;
        }

        throw new UnknownColumnException(clauseType, colRef);
      }

      final SourceName source = colRef.maybeQualifier()
          .orElseGet(() -> {
            if (isWindowBound(colRef.getColumnName()) && sourcesWithField.size() > 1) {
              // Window bounds are not yet supported in the projection of windowed aggregates with
              // joins. See https://github.com/confluentinc/ksql/issues/5931
              throw new UnknownColumnException(clauseType, colRef);
            }

            // AstSanitizer catches ambiguous columns
            return Iterables.getOnlyElement(sourcesWithField);
          });

      referencedSources.add(source);
    }

    private boolean couldBeSyntheticJoinColumn(final ColumnReferenceExp colRef) {
      if (!possibleSyntheticColumns) {
        // Some queries never have synthetic columns, e.g. pull or aggregations.
        return false;
      }

      if (!sourceSchemas.isJoin()) {
        // Synthetic join columns only occur in joins... duh!
        return false;
      }

      if (colRef instanceof QualifiedColumnReferenceExp) {
        // Synthetic join columns can't be qualified, as they don't belong to any source
        return false;
      }

      if (!clauseType.equals("SELECT")) {
        // Synthetic join columns can only be used in the projection
        return false;
      }

      // Synthetic join columns have generated names:
      return ColumnNames.maybeSyntheticJoinKey(colRef.getColumnName());
    }
  }
}
