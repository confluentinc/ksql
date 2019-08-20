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

package io.confluent.ksql.parser;

import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SimpleGroupBy;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statements;

public abstract class DefaultTraversalVisitor<R, C> extends AstVisitor<R, C> {

  @Override
  protected R visitStatements(final Statements node, final C context) {
    node.getStatements()
        .forEach(stmt -> process(stmt, context));
    return visitNode(node, context);
  }

  @Override
  protected R visitQuery(final Query node, final C context) {
    process(node.getSelect(), context);
    process(node.getFrom(), context);

    if (node.getGroupBy().isPresent()) {
      process(node.getGroupBy().get(), context);
    }
    return null;
  }

  @Override
  protected R visitSelect(final Select node, final C context) {
    for (final SelectItem item : node.getSelectItems()) {
      process(item, context);
    }

    return null;
  }

  @Override
  protected R visitSingleColumn(final SingleColumn node, final C context) {
    return null;
  }

  @Override
  protected R visitAliasedRelation(final AliasedRelation node, final C context) {
    return process(node.getRelation(), context);
  }

  @Override
  protected R visitJoin(final Join node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    return null;
  }

  @Override
  protected R visitGroupBy(final GroupBy node, final C context) {
    for (final GroupingElement groupingElement : node.getGroupingElements()) {
      process(groupingElement, context);
    }

    return null;
  }

  @Override
  protected R visitGroupingElement(final GroupingElement node, final C context) {
    return null;
  }

  @Override
  protected R visitSimpleGroupBy(final SimpleGroupBy node, final C context) {
    visitGroupingElement(node, context);

    return null;
  }

  @Override
  protected R visitInsertInto(final InsertInto node, final C context) {
    process(node.getQuery(), context);
    return null;
  }
}
