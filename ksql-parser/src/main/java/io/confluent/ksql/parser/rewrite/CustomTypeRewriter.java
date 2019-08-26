/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.rewrite;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.parser.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlCustomType;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Optional;

public class CustomTypeRewriter {

  private final TypeRegistry typeRegistry;
  private final Statement statement;

  public CustomTypeRewriter(
      final TypeRegistry typeRegistry,
      final Statement statement
  ) {
    this.typeRegistry = Objects.requireNonNull(typeRegistry, "typeRegistry");
    this.statement = Objects.requireNonNull(statement, "statement");
  }

  public static boolean requiresRewrite(final Statement statement) {
    return statement instanceof CreateAsSelect
        || statement instanceof CreateSource
        || statement instanceof RegisterType;
  }

  public Statement rewrite() {
    return (Statement) new StatementRewriter<>(
        (e, c) -> ExpressionTreeRewriter.rewriteWith(new Plugin(typeRegistry)::process, e)
    ).rewrite(statement, null);
  }

  private static final class Plugin
      extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    private final TypeRegistry typeRegistry;

    Plugin(final TypeRegistry typeRegistry) {
      super(Optional.empty());
      this.typeRegistry = Objects.requireNonNull(typeRegistry, "typeRegistry");
    }

    @Override
    public Optional<Expression> visitType(final Type node, final Context<Void> context) {
      switch (node.getSqlType().baseType()) {
        case ARRAY:
          return visitArray((SqlArray) node.getSqlType(), context);
        case MAP:
          return visitMap((SqlMap) node.getSqlType(), context);
        case STRUCT:
          return visitStruct((SqlStruct) node.getSqlType(), context);
        case CUSTOM:
          return visitCustomType((SqlCustomType) node.getSqlType(), context);
        default:
          return Optional.of(node);
      }
    }

    private Optional<Expression> visitArray(final SqlArray array, final Context<Void> context) {
      final SqlType itemType = array.getItemType();
      final Type inner =
          (Type) visitType(new Type(itemType), context)
              .orElseThrow(() -> new KsqlException("Expected type visit to return non-empty!"));

      return Optional.of(new Type(SqlArray.of(inner.getSqlType())));
    }

    private Optional<Expression> visitMap(final SqlMap map, final Context<Void> context) {
      final SqlType itemType = map.getValueType();
      final Type value =
          (Type) visitType(new Type(itemType), context)
              .orElseThrow(() -> new KsqlException("Expected type visit to return non-empty!"));

      return Optional.of(new Type(SqlMap.of(value.getSqlType())));
    }

    private Optional<Expression> visitStruct(final SqlStruct struct, final Context<Void> context) {
      final SqlStruct.Builder structBuilder = SqlStruct.builder();
      for (final Field field : struct.getFields()) {
        final Type fieldType = (Type) visitType(new Type(field.type()), context)
            .orElseThrow(() -> new KsqlException("Expected type visit to return non-empty!"));
        structBuilder.field(field.name(), fieldType.getSqlType());
      }
      return Optional.of(new Type(structBuilder.build()));
    }

    private Optional<Expression> visitCustomType(
        final SqlCustomType alias,
        final Context<Void> context
    ) {
      final Optional<SqlType> sqlType = typeRegistry.resolveType(alias.getAlias());

      final Optional<Expression> rewritten = sqlType.map(Type::new).map(Expression.class::cast);
      if (rewritten.isPresent()) {
        return rewritten;
      }

      throw new KsqlException("Cannot resolve unknown type or alias: " + alias.getAlias());
    }
  }

}
