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

package io.confluent.ksql.execution.codegen.helpers;

import io.confluent.ksql.schema.ksql.SqlTypeWalker;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Field;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generates Java code to build an {@code SqlType}.
 */
public final class SqlTypeCodeGen {

  private SqlTypeCodeGen() {
  }

  public static String generateCode(final SqlType sqlType) {
    return SqlTypeWalker.visit(sqlType, new TypeVisitor());
  }

  private static class TypeVisitor implements SqlTypeWalker.Visitor<String, String> {

    @Override
    public String visitBoolean(final SqlPrimitiveType type) {
      return "SqlTypes.BOOLEAN";
    }

    @Override
    public String visitInt(final SqlPrimitiveType type) {
      return "SqlTypes.INTEGER";
    }

    @Override
    public String visitBigInt(final SqlPrimitiveType type) {
      return "SqlTypes.BIGINT";
    }

    @Override
    public String visitDouble(final SqlPrimitiveType type) {
      return "SqlTypes.DOUBLE";
    }

    @Override
    public String visitString(final SqlPrimitiveType type) {
      return "SqlTypes.STRING";
    }

    @Override
    public String visitTimestamp(final SqlPrimitiveType type) {
      return "SqlTypes.TIMESTAMP";
    }

    @Override
    public String visitDecimal(final SqlDecimal type) {
      return "SqlTypes.decimal(" + type.getPrecision() + "," + type.getScale() + ")";
    }

    @Override
    public String visitArray(final SqlArray type, final String element) {
      return "SqlTypes.array(" + element + ")";
    }

    @Override
    public String visitMap(final SqlMap type, final String key, final String value) {
      return "SqlTypes.map(" + key + "," + value + ")";
    }

    @Override
    public String visitStruct(final SqlStruct type, final List<? extends String> fields) {
      return fields.stream()
          .collect(Collectors.joining(
              ").field(",
              "SqlTypes.struct().field(",
              ").build()"
          ));
    }

    @Override
    public String visitField(final Field field, final String type) {
      return "\"" + field.name() + "\"," + type;
    }
  }
}
