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

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import io.confluent.ksql.parser.SqlBaseBaseVisitor;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.NodeLocation;
import io.confluent.ksql.parser.tree.Table;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class DataSourceExtractor {

  private final MetaStore metaStore;

  private Schema joinLeftSchema;
  private Schema joinRightSchema;

  private String fromAlias;
  private String fromName;
  private String leftAlias;
  private String leftName;
  private String rightAlias;
  private String rightName;

  private final Set<String> commonFieldNames = new HashSet<>();
  private final Set<String> leftFieldNames = new HashSet<>();
  private final Set<String> rightFieldNames = new HashSet<>();

  private boolean isJoin = false;

  public DataSourceExtractor(final MetaStore metaStore) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
  }

  public void extractDataSources(final ParseTree node) {
    new Visitor().visit(node);

    if (joinLeftSchema != null) {
      for (final Field field : joinLeftSchema.fields()) {
        leftFieldNames.add(field.name());
      }
      for (final Field field : joinRightSchema.fields()) {
        rightFieldNames.add(field.name());
        if (leftFieldNames.contains(field.name())) {
          commonFieldNames.add(field.name());
        }
      }
    }
  }

  public MetaStore getMetaStore() {
    return metaStore;
  }

  public String getFromAlias() {
    return fromAlias;
  }

  public String getLeftAlias() {
    return leftAlias;
  }

  public String getRightAlias() {
    return rightAlias;
  }

  public Set<String> getCommonFieldNames() {
    return commonFieldNames;
  }

  public Set<String> getLeftFieldNames() {
    return leftFieldNames;
  }

  public Set<String> getRightFieldNames() {
    return rightFieldNames;
  }

  public String getFromName() {
    return fromName;
  }

  public String getLeftName() {
    return leftName;
  }

  public String getRightName() {
    return rightName;
  }

  public boolean isJoin() {
    return isJoin;
  }

  private final class Visitor extends SqlBaseBaseVisitor<Node> {

    @Override
    public Node visitQuery(final SqlBaseParser.QueryContext ctx) {
      visit(ctx.from);
      return visitChildren(ctx);
    }

    @Override
    public Node visitTableName(final SqlBaseParser.TableNameContext context) {
      return new Table(getLocation(context), ParserUtil.getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitAliasedRelation(final SqlBaseParser.AliasedRelationContext context) {
      final Table table = (Table) visit(context.relationPrimary());

      String alias = null;
      if (context.children.size() == 1) {
        alias = table.getName().getSuffix().toUpperCase();

      } else if (context.children.size() == 2) {
        alias = context.children.get(1).getText().toUpperCase();
      }

      if (!isJoin) {
        fromAlias = alias;
        fromName = table.getName().getSuffix().toUpperCase();
        if (metaStore.getSource(table.getName().getSuffix()) == null) {
          throw new KsqlException(table.getName().getSuffix() + " does not exist.");
        }

        return null;
      }

      return new AliasedRelation(getLocation(context), table, alias);
    }

    @Override
    public Node visitJoinRelation(final SqlBaseParser.JoinRelationContext context) {
      isJoin = true;
      final AliasedRelation left = (AliasedRelation) visit(context.left);
      leftAlias = left.getAlias();
      leftName = ((Table) left.getRelation()).getName().getSuffix();
      final StructuredDataSource
          leftDataSource =
          metaStore.getSource(((Table) left.getRelation()).getName().getSuffix());
      if (leftDataSource == null) {
        throw new KsqlException(((Table) left.getRelation()).getName().getSuffix() + " does not "
            + "exist.");
      }
      joinLeftSchema = leftDataSource.getSchema();

      final AliasedRelation right = (AliasedRelation) visit(context.right);
      rightAlias = right.getAlias();
      rightName = ((Table) right.getRelation()).getName().getSuffix();
      final StructuredDataSource
          rightDataSource =
          metaStore.getSource(((Table) right.getRelation()).getName().getSuffix());
      if (rightDataSource == null) {
        throw new KsqlException(((Table) right.getRelation()).getName().getSuffix() + " does not "
            + "exist.");
      }
      joinRightSchema = rightDataSource.getSchema();

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
}
