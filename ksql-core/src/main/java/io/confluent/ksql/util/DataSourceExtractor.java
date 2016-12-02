package io.confluent.ksql.util;

import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.SqlBaseBaseVisitor;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.*;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DataSourceExtractor
    extends SqlBaseBaseVisitor<Node> {

  final MetaStore metaStore;

  Schema fromSchema;
  Schema joinLeftSchema;
  Schema joinRightSchema;

  String fromAlias;
  String leftAlias;
  String rightAlias;

  Set<String> commonFieldNames = new HashSet<>();
  Set<String> leftFieldNames = new HashSet<>();
  Set<String> rightFieldNames = new HashSet<>();

  boolean isJoin = false;

  public DataSourceExtractor(MetaStore metaStore) {

    this.metaStore = metaStore;
  }

  @Override
  public Node visitQuerySpecification(SqlBaseParser.QuerySpecificationContext ctx) {
    Relation from = (Relation) visit(ctx.from);
    return visitChildren(ctx);
  }

  @Override
  public Node visitTableName(SqlBaseParser.TableNameContext context) {
    return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
  }

  @Override
  public Node visitAliasedRelation(SqlBaseParser.AliasedRelationContext context) {
    Table table = (Table) visit(context.relationPrimary());

    String alias = null;
    if (context.children.size() == 1) {
      alias = table.getName().getSuffix();

    } else if (context.children.size() == 2) {
      alias = context.children.get(1).getText();
    }

    if (!isJoin) {
      this.fromAlias = alias.toUpperCase();
      DataSource fromDataSource = metaStore.getSource(table.getName().getSuffix().toUpperCase());
      this.fromSchema = fromDataSource.getSchema();
      return null;
    }

    return new AliasedRelation(getLocation(context), table, alias,
                               getColumnAliases(context.columnAliases()));

  }

  @Override
  public Node visitJoinRelation(SqlBaseParser.JoinRelationContext context) {
    this.isJoin = true;
    AliasedRelation left = (AliasedRelation) visit(context.left);
    AliasedRelation right;
    if (context.CROSS() != null) {
      right = (AliasedRelation) visit(context.right);
    } else {
      if (context.NATURAL() != null) {
        right = (AliasedRelation) visit(context.right);
      } else {
        right = (AliasedRelation) visit(context.rightRelation);
      }
    }

    this.leftAlias = left.getAlias().toUpperCase();
    DataSource
        leftDataSource =
        metaStore.getSource(((Table) left.getRelation()).getName().getSuffix().toUpperCase());
    this.joinLeftSchema = leftDataSource.getSchema();

    this.rightAlias = right.getAlias().toUpperCase();
    DataSource
        rightDataSource =
        metaStore.getSource(((Table) right.getRelation()).getName().getSuffix().toUpperCase());
    this.joinRightSchema = rightDataSource.getSchema();

    return null;
  }


  public void extractDataSources(ParseTree node) {
    visit(node);
    if (joinLeftSchema != null) {
      for (Field field : joinLeftSchema.fields()) {
        leftFieldNames.add(field.name().toUpperCase());
      }
      for (Field field : joinRightSchema.fields()) {
        rightFieldNames.add(field.name().toUpperCase());
        if (leftFieldNames.contains(field.name().toUpperCase())) {
          commonFieldNames.add(field.name().toUpperCase());
        }
      }
    }
  }

  public MetaStore getMetaStore() {
    return metaStore;
  }

  public Schema getFromSchema() {
    return fromSchema;
  }

  public Schema getJoinLeftSchema() {
    return joinLeftSchema;
  }

  public Schema getJoinRightSchema() {
    return joinRightSchema;
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


  private static QualifiedName getQualifiedName(SqlBaseParser.QualifiedNameContext context) {
    List<String> parts = context
        .identifier().stream()
        .map(ParseTree::getText)
        .collect(toList());

    return QualifiedName.of(parts);
  }

  private static boolean isDistinct(SqlBaseParser.SetQuantifierContext setQuantifier) {
    return setQuantifier != null && setQuantifier.DISTINCT() != null;
  }

  private static Optional<String> getTextIfPresent(ParserRuleContext context) {
    return Optional.ofNullable(context)
        .map(ParseTree::getText);
  }

  private static Optional<String> getTextIfPresent(Token token) {
    return Optional.ofNullable(token)
        .map(Token::getText);
  }

  private static List<String> getColumnAliases(
      SqlBaseParser.ColumnAliasesContext columnAliasesContext) {
    if (columnAliasesContext == null) {
      return null;
    }

    return columnAliasesContext
        .identifier().stream()
        .map(ParseTree::getText)
        .collect(toList());
  }

  public static NodeLocation getLocation(TerminalNode terminalNode) {
    requireNonNull(terminalNode, "terminalNode is null");
    return getLocation(terminalNode.getSymbol());
  }

  public static NodeLocation getLocation(ParserRuleContext parserRuleContext) {
    requireNonNull(parserRuleContext, "parserRuleContext is null");
    return getLocation(parserRuleContext.getStart());
  }

  public static NodeLocation getLocation(Token token) {
    requireNonNull(token, "token is null");
    return new NodeLocation(token.getLine(), token.getCharPositionInLine());
  }
}
