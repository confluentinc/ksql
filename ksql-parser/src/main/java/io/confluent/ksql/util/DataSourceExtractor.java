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

package io.confluent.ksql.util;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.Sets;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.SqlBaseBaseVisitor;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AstNode;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;

public class DataSourceExtractor {

  private final MetaStore metaStore;

  private SourceName fromAlias;
  private SourceName fromName;
  private SourceName leftAlias;
  private SourceName leftName;
  private SourceName rightAlias;
  private SourceName rightName;

  private final Set<ColumnName> commonFieldNames = new HashSet<>();
  private final Set<ColumnName> leftFieldNames = new HashSet<>();
  private final Set<ColumnName> rightFieldNames = new HashSet<>();

  private boolean isJoin = false;

  public DataSourceExtractor(final MetaStore metaStore) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
  }

  public void extractDataSources(final ParseTree node) {
    new Visitor().visit(node);

    commonFieldNames.addAll(Sets.intersection(leftFieldNames, rightFieldNames));
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

  public Set<ColumnName> getCommonFieldNames() {
    return Collections.unmodifiableSet(commonFieldNames);
  }

  public Set<ColumnName> getLeftFieldNames() {
    return Collections.unmodifiableSet(leftFieldNames);
  }

  public Set<ColumnName> getRightFieldNames() {
    return Collections.unmodifiableSet(rightFieldNames);
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

  private final class Visitor extends SqlBaseBaseVisitor<AstNode> {

    @Override
    public AstNode visitQuery(final SqlBaseParser.QueryContext ctx) {
      visit(ctx.from);
      return visitChildren(ctx);
    }

    @Override
    public AstNode visitTableName(final SqlBaseParser.TableNameContext context) {
      return new Table(
          getLocation(context),
          SourceName.of(ParserUtil.getIdentifierText(context.identifier())));
    }

    @Override
    public AstNode visitAliasedRelation(final SqlBaseParser.AliasedRelationContext context) {
      final Table table = (Table) visit(context.relationPrimary());

      final String alias;
      switch (context.children.size()) {
        case 1:
          alias = table.getName().name().toUpperCase();
          break;

        case 2:
          alias = context.children.get(1).getText().toUpperCase();
          break;

        case 3:
          alias = context.children.get(2).getText().toUpperCase();
          break;

        default:
          throw new IllegalArgumentException(
              "AliasedRelationContext must have between 1 and 3 children, but has:"
                  + context.children.size()
          );
      }

      if (!isJoin) {
        fromAlias = SourceName.of(alias);
        fromName = SourceName.of(table.getName().name().toUpperCase());
        if (metaStore.getSource(fromName) == null) {
          throw new KsqlException(table.getName().name() + " does not exist.");
        }

        return null;
      }

      return new AliasedRelation(getLocation(context), table, SourceName.of(alias.toUpperCase()));
    }

    @Override
    public AstNode visitJoinRelation(final SqlBaseParser.JoinRelationContext context) {
      isJoin = true;
      final AliasedRelation left = (AliasedRelation) visit(context.left);
      leftAlias = left.getAlias();
      leftName = ((Table) left.getRelation()).getName();
      final DataSource
          leftDataSource =
          metaStore.getSource(((Table) left.getRelation()).getName());
      if (leftDataSource == null) {
        throw new KsqlException(((Table) left.getRelation()).getName().name() + " does not "
            + "exist.");
      }
      addFieldNames(leftDataSource.getSchema(), leftFieldNames);

      final AliasedRelation right = (AliasedRelation) visit(context.right);
      rightAlias = right.getAlias();
      rightName = ((Table) right.getRelation()).getName();
      final DataSource
          rightDataSource =
          metaStore.getSource(((Table) right.getRelation()).getName());
      if (rightDataSource == null) {
        throw new KsqlException(((Table) right.getRelation()).getName().name() + " does not "
            + "exist.");
      }
      addFieldNames(rightDataSource.getSchema(), rightFieldNames);

      return null;
    }
  }

  private static Optional<NodeLocation> getLocation(final ParserRuleContext parserRuleContext) {
    requireNonNull(parserRuleContext, "parserRuleContext is null");
    return getLocation(parserRuleContext.getStart());
  }

  private static Optional<NodeLocation> getLocation(final Token token) {
    requireNonNull(token, "token is null");
    return Optional.of(new NodeLocation(token.getLine(), token.getCharPositionInLine()));
  }

  private static void addFieldNames(final LogicalSchema schema, final Set<ColumnName> collection) {
    schema.columns().forEach(field -> collection.add(field.name()));
  }
}
