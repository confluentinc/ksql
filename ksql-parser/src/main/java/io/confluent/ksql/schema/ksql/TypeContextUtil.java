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

import io.confluent.ksql.parser.CaseInsensitiveStream;
import io.confluent.ksql.parser.SqlBaseLexer;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.SqlBaseParser.TypeContext;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.Struct.Builder;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.ParserUtil;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;

public final class TypeContextUtil {

  private TypeContextUtil() { }

  public static Type getType(final String schema) {
    return getType(parseTypeContext(schema));
  }

  public static Type getType(final SqlBaseParser.TypeContext type) {
    if (type.baseType() != null) {
      return PrimitiveType.of(baseTypeToString(type.baseType()));
    }

    if (type.ARRAY() != null) {
      return io.confluent.ksql.parser.tree.Array.of(getType(type.type(0)));
    }

    if (type.MAP() != null) {
      return io.confluent.ksql.parser.tree.Map.of(getType(type.type(1)));
    }

    if (type.STRUCT() != null) {
      final Builder builder = Struct.builder();

      for (int i = 0; i < type.identifier().size(); i++) {
        final String fieldName = ParserUtil.getIdentifierText(type.identifier(i));
        final Type fieldType = getType(type.type(i));
        builder.addField(fieldName, fieldType);
      }
      return builder.build();
    }

    throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
  }

  private static TypeContext parseTypeContext(final String schema) {
    final SqlBaseLexer lexer = new SqlBaseLexer(
        new CaseInsensitiveStream(CharStreams.fromString(schema)));
    final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    final SqlBaseParser parser = new SqlBaseParser(tokenStream);
    parser.getInterpreter().setPredictionMode(PredictionMode.LL);
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

}
