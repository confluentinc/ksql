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

package io.confluent.ksql.schema.ksql;

import static io.confluent.ksql.parser.DefaultKsqlParser.ERROR_VALIDATOR;

import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.parser.CaseInsensitiveStream;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.ParsingException;
import io.confluent.ksql.parser.SqlBaseLexer;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.SqlBaseParser.TypeContext;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlConstraint;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.ParserUtil;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ErrorNode;

public final class SqlTypeParser {

  private final TypeRegistry typeRegistry;

  public static SqlTypeParser create(final TypeRegistry typeRegistry) {
    return new SqlTypeParser(typeRegistry);
  }

  private SqlTypeParser(final TypeRegistry typeRegistry) {
    this.typeRegistry = Objects.requireNonNull(typeRegistry, "typeRegistry");
  }

  public Type parse(final String schema) {
    try {
      final TypeContext typeContext = parseTypeContext(schema);
      return getType(typeContext);
    } catch (final ParsingException e) {
      throw new KsqlException("Failed to parse: " + schema, e);
    }
  }

  public Type getType(
      final SqlBaseParser.TypeContext type
  ) {
    final Optional<NodeLocation> location = ParserUtil.getLocation(type);
    final SqlType sqlType = getSqlType(type);
    return new Type(location, sqlType);
  }

  private SqlType getSqlType(final SqlBaseParser.TypeContext type) {
    if (type.baseType() != null) {
      final String baseType = baseTypeToString(type.baseType());
      if (SqlPrimitiveType.isPrimitiveTypeName(baseType)) {
        final SqlPrimitiveType sqlPrimitiveType = SqlPrimitiveType.of(baseType);
        if (isNotNull(type.baseType().constraint())) {
          return sqlPrimitiveType.required();
        }
        return sqlPrimitiveType;
      } else {
        return typeRegistry
            .resolveType(baseType)
            .orElseThrow(() -> new KsqlException("Cannot resolve unknown type: " + baseType));
      }
    }

    if (type.DECIMAL() != null) {
      return toDecimal(type);
    }

    if (type.ARRAY() != null) {
      return toArray(type);
    }

    if (type.MAP() != null) {
      return toMap(type);
    }

    if (type.STRUCT() != null) {
      return toStruct(type);
    }

    throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
  }

  private SqlType toDecimal(final SqlBaseParser.TypeContext type) {
    final SqlDecimal sqlDecimal = SqlDecimal.of(
            ParserUtil.processIntegerNumber(type.number(0), "DECIMAL(PRECISION)"),
            ParserUtil.processIntegerNumber(type.number(1), "DECIMAL(SCALE)")
    );
    if (isNotNull(type.constraint())) {
      return sqlDecimal.required();
    }
    return sqlDecimal;
  }

  private SqlType toArray(final SqlBaseParser.TypeContext type) {
    final SqlArray sqlArray = SqlArray.of(getSqlType(type.type(0)));
    if (isNotNull(type.constraint())) {
      return sqlArray.required();
    }
    return sqlArray;
  }

  private SqlType toMap(final SqlBaseParser.TypeContext type) {
    final SqlMap sqlMap = SqlMap.of(getSqlType(type.type(0)), getSqlType(type.type(1)));
    if (isNotNull(type.constraint())) {
      return sqlMap.required();
    }
    return sqlMap;
  }

  private SqlType toStruct(final SqlBaseParser.TypeContext type) {
    final SqlStruct.Builder builder = SqlStruct.builder();

    for (int i = 0; i < type.identifier().size(); i++) {
      final String fieldName = ParserUtil.getIdentifierText(type.identifier(i));
      final SqlType fieldType = getSqlType(type.type(i));
      builder.field(fieldName, fieldType);
    }
    final SqlStruct sqlStruct = builder.build();
    if (isNotNull(type.constraint())) {
      return sqlStruct.required();
    }
    return sqlStruct;
  }

  private static TypeContext parseTypeContext(final String schema) {
    final SqlBaseLexer lexer = new SqlBaseLexer(
        new CaseInsensitiveStream(CharStreams.fromString(schema)));
    final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    final SqlBaseParser parser = new SqlBaseParser(tokenStream);
    parser.getInterpreter().setPredictionMode(PredictionMode.LL);
    parser.removeErrorListeners();
    parser.addErrorListener(ERROR_VALIDATOR);
    return parser.type();
  }

  private static String baseTypeToString(final SqlBaseParser.BaseTypeContext baseType) {
    if (baseType.identifier() != null) {
      return ParserUtil.getIdentifierText(baseType.identifier());
    } else {
      throw new KsqlException(
          "Base type must contain either identifier, "
              + "time with time zone, or timestamp with time zone"
      );
    }
  }

  private static boolean isNotNull(final SqlBaseParser.ConstraintContext constraintContext) {
    if (constraintContext == null) {
      return false;
    }
    final String constraint = constraintContext.children.stream()
            .filter(i -> !(i instanceof ErrorNode))
            .map(i -> i.getText().toUpperCase())
            .collect(Collectors.joining());
    try {
      final SqlConstraint sqlConstraint = SqlConstraint.valueOf(constraint);
      if (SqlConstraint.NOTNULL.equals(sqlConstraint)) {
        return true;
      }
    } catch (final IllegalArgumentException e) {
      throw new KsqlException("Unknown constraint");
    }
    return false;
  }
}
