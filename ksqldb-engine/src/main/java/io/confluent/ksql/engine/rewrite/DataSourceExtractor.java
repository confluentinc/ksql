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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
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
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.util.KsqlException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DataSourceExtractor {

  private final MetaStore metaStore;

  private final Set<AliasedDataSource> allSources = new HashSet<>();
  private final Set<ColumnName> allColumns = new HashSet<>();
  private final Set<ColumnName> clashingColumns = new HashSet<>();

  private boolean isJoin = false;

  public DataSourceExtractor(final MetaStore metaStore) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
  }

  public Set<Analysis.AliasedDataSource> extractDataSources(final AstNode node) {
    new Visitor().process(node, null);
    return getAllSources();
  }

  public Set<AliasedDataSource> getAllSources() {
    return ImmutableSet.copyOf(allSources);
  }

  /**
   * @param name the column name to test.
   * @return {@code true} if the name exists in more than one source.
   */
  public boolean isClashingColumnName(final ColumnName name) {
    if (!isJoin) {
      return false;
    }

    if (SystemColumns.isPseudoColumn(name)) {
      return true;
    }

    return clashingColumns.contains(name);
  }

  public List<SourceName> getSourcesFor(final ColumnName columnName) {
    return allSources.stream()
        .filter(aliased -> hasColumn(columnName, aliased))
        .map(AliasedDataSource::getAlias)
        .collect(Collectors.toList());
  }

  private static boolean hasColumn(
      final ColumnName columnName,
      final AliasedDataSource aliased
  ) {
    if (SystemColumns.isPseudoColumn(columnName)) {
      return true;
    }

    if (aliased.getDataSource().getKsqlTopic().getKeyFormat().isWindowed()
        && SystemColumns.isWindowBound(columnName)
    ) {
      return true;
    }

    return aliased.getDataSource().getSchema().findColumn(columnName).isPresent();
  }

  private final class Visitor extends DefaultTraversalVisitor<Void, Void> {
    @Override
    public Void visitRelation(final Relation relation, final Void ctx) {
      throw new IllegalStateException("Unexpected source relation");
    }

    @Override
    public Void visitAliasedRelation(final AliasedRelation relation, final Void ctx) {
      final SourceName fromName = ((Table) relation.getRelation()).getName();
      final DataSource source = metaStore.getSource(fromName);
      if (source == null) {
        throw new KsqlException(fromName.text() + " does not exist.");
      }

      allSources.add(new AliasedDataSource(relation.getAlias(), source));

      final Set<ColumnName> columns = source
          .getSchema()
          .columns()
          .stream()
          .map(Column::name)
          .collect(Collectors.toSet());

      clashingColumns.addAll(Sets.intersection(allColumns, columns));

      allColumns.addAll(columns);

      return null;
    }

    @Override
    public Void visitJoin(final Join join, final Void ctx) {
      isJoin = true;
      return super.visitJoin(join, ctx);
    }
  }
}
