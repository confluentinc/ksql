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

package io.confluent.ksql.function.udf.json;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.VALUE_FALSE;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_FLOAT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_INT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static com.fasterxml.jackson.core.JsonToken.VALUE_TRUE;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Objects;
import java.util.function.Predicate;

@UdfDescription(
    name = "JSON_ARRAY_CONTAINS",
    category = FunctionCategory.JSON,
    description = JsonArrayContains.DESCRIPTION,
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class JsonArrayContains {

  static final String DESCRIPTION = "Parses a JSON array and determines whether or not the "
      + "supplied value is contained within the array.";

  private static final JsonFactory PARSER_FACTORY = new JsonFactoryBuilder()
      .disable(CANONICALIZE_FIELD_NAMES)
      .build()
      .setCodec(UdfJsonMapper.INSTANCE);

  private static final EnumMap<JsonToken, Predicate<Object>> TOKEN_COMPAT;

  static {
    TOKEN_COMPAT = new EnumMap<>(JsonToken.class);
    TOKEN_COMPAT.put(VALUE_NUMBER_INT, obj -> obj instanceof Long || obj instanceof Integer);
    TOKEN_COMPAT.put(VALUE_NUMBER_FLOAT, Double.class::isInstance);
    TOKEN_COMPAT.put(VALUE_STRING, String.class::isInstance);
    TOKEN_COMPAT.put(VALUE_TRUE, obj -> obj instanceof Boolean && (Boolean) obj);
    TOKEN_COMPAT.put(VALUE_FALSE, obj -> obj instanceof Boolean && !(Boolean) obj);
    TOKEN_COMPAT.put(VALUE_NULL, Objects::isNull);
  }

  @Udf
  public <T> Boolean contains(
      @UdfParameter final String jsonArray,
      @UdfParameter final T val
  ) {
    try (JsonParser parser = PARSER_FACTORY.createParser(jsonArray)) {
      if (parser.nextToken() != START_ARRAY) {
        return false;
      }

      while (parser.nextToken() != null) {
        final JsonToken token = parser.currentToken();

        if (token == null) {
          return val == null;
        } else if (token == END_ARRAY) {
          return false;
        }

        parser.skipChildren();
        if (TOKEN_COMPAT.getOrDefault(token, foo -> false).test(val)) {
          if (token == VALUE_NULL
              || (val != null && Objects.equals(parser.readValueAs(val.getClass()), val))) {
            return true;
          }
        }
      }

      return false;
    } catch (final IOException e) {
      return false;
    }
  }

}
