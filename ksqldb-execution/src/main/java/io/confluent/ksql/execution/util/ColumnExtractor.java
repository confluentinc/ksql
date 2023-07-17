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

package io.confluent.ksql.execution.util;

import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import java.util.HashSet;
import java.util.Set;

/**
 * Extracts column references from expressions
 */
public final class ColumnExtractor {

  private ColumnExtractor() {
  }

  public static Set<? extends ColumnReferenceExp> extractColumns(final Expression e) {
    final Set<ColumnReferenceExp> references = new HashSet<>();
    new Extractor().process(e, references);
    return references;
  }

  private static final class Extractor extends TraversalExpressionVisitor<Set<ColumnReferenceExp>> {

    @Override
    public Void visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Set<ColumnReferenceExp> references
    ) {
      references.add(node);
      return null;
    }

    @Override
    public Void visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Set<ColumnReferenceExp> references
    ) {
      references.add(node);
      return null;
    }
  }
}
