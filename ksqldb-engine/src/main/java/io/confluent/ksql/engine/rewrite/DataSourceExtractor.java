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

package io.confluent.ksql.engine.rewrite;

import com.google.common.collect.Sets;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.DefaultTraversalVisitor;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AstNode;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

class DataSourceExtractor {

  private final MetaStore metaStore;

  private SourceName fromAlias;
  private SourceName fromName;
  private SourceName leftAlias;
  private SourceName leftName;
  private SourceName rightAlias;
  private SourceName rightName;

  private final Set<DataSource> allSources = new HashSet<>();
  private final Set<ColumnName> commonColumnNames = new HashSet<>();
  private final Set<ColumnName> leftColumnNames = new HashSet<>();
  private final Set<ColumnName> rightColumnNames = new HashSet<>();

  private boolean isJoin = false;

  DataSourceExtractor(final MetaStore metaStore) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
  }

  public void extractDataSources(final AstNode node) {
    new Visitor().process(node, null);
    commonColumnNames.addAll(Sets.intersection(leftColumnNames, rightColumnNames));
  }

  public SourceName getFromAlias() {
    return fromAlias;
  }

  public SourceName getLeftAlias() {
    return leftAlias;
  }

  public SourceName getRightAlias() {
    return rightAlias;
  }

  public Set<DataSource> getAllSources() {
    return Collections.unmodifiableSet(allSources);
  }

  public Set<ColumnName> getCommonColumnNames() {
    return Collections.unmodifiableSet(commonColumnNames);
  }

  public SourceName getFromName() {
    return fromName;
  }

  public SourceName getLeftName() {
    return leftName;
  }

  public SourceName getRightName() {
    return rightName;
  }

  public boolean isJoin() {
    return isJoin;
  }

  public SourceName getAliasFor(final ColumnName columnName) {
    if (isJoin) {
      if (commonColumnNames.contains(columnName)) {
        throw new KsqlException("Column '" + columnName.text() + "' is ambiguous.");
      }

      if (leftColumnNames.contains(columnName)) {
        return leftAlias;
      }

      if (rightColumnNames.contains(columnName)) {
        return rightAlias;
      }

      throw new KsqlException(
          "Column '" + columnName.text() + "' cannot be resolved."
      );
    }
    return fromAlias;
  }

  private final class Visitor extends DefaultTraversalVisitor<Void, Void> {
    @Override
    public Void visitRelation(final Relation relation, final Void ctx) {
      throw new IllegalStateException("Unexpected source relation");
    }

    @Override
    public Void visitAliasedRelation(final AliasedRelation relation, final Void ctx) {
      fromAlias = relation.getAlias();
      fromName = ((Table) relation.getRelation()).getName();
      final DataSource source = metaStore.getSource(fromName);
      if (source == null) {
        throw new KsqlException(fromName.text() + " does not exist.");
      }

      allSources.add(source);
      return null;
    }

    @Override
    public Void visitJoin(final Join join, final Void ctx) {
      isJoin = true;
      final AliasedRelation left = (AliasedRelation) join.getLeft();
      leftAlias = left.getAlias();
      leftName = ((Table) left.getRelation()).getName();
      final DataSource
          leftDataSource =
          metaStore.getSource(((Table) left.getRelation()).getName());
      if (leftDataSource == null) {
        throw new KsqlException(((Table) left.getRelation()).getName().text() + " does not "
            + "exist.");
      }
      addFieldNames(leftDataSource.getSchema(), leftColumnNames);
      final AliasedRelation right = (AliasedRelation) join.getRight();
      rightAlias = right.getAlias();
      rightName = ((Table) right.getRelation()).getName();
      final DataSource
          rightDataSource =
          metaStore.getSource(((Table) right.getRelation()).getName());
      if (rightDataSource == null) {
        throw new KsqlException(((Table) right.getRelation()).getName().text() + " does not "
            + "exist.");
      }
      addFieldNames(rightDataSource.getSchema(), rightColumnNames);
      allSources.add(leftDataSource);
      allSources.add(rightDataSource);
      return null;
    }
  }

  private static void addFieldNames(final LogicalSchema schema, final Set<ColumnName> collection) {
    schema.columns().forEach(field -> collection.add(field.name()));
  }
}
