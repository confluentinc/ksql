/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function.udf.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.util.ArrayUtil;
import io.confluent.ksql.util.KsqlException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.VALUE_FALSE;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_FLOAT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_INT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static com.fasterxml.jackson.core.JsonToken.VALUE_TRUE;

public class ArrayContainsKudf implements Kudf {
  private static final JsonFactory JSON_FACTORY =
      new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);


  interface Matcher {
    boolean matches(final JsonParser parser,
                    final Object searchValue) throws IOException;
  }

  private final Map<JsonToken, Matcher> matchers = new HashMap<>();


  ArrayContainsKudf() {
    matchers.put(JsonToken.VALUE_NULL, (parser, value) -> value == null);
    matchers.put(JsonToken.VALUE_STRING,
        (parser, value) -> parser.getText().equals(value));
    final Matcher booleanMatcher =
        (parser, value) -> parser.getBooleanValue() == (boolean) value;
    matchers.put(JsonToken.VALUE_FALSE, booleanMatcher);
    matchers.put(JsonToken.VALUE_TRUE, booleanMatcher);
    matchers.put(JsonToken.VALUE_NUMBER_INT,
        (parser, value) ->
            value instanceof Integer && parser.getIntValue() == (int) value
                || value instanceof Long && parser.getLongValue() == (long) value);
    matchers.put(JsonToken.VALUE_NUMBER_FLOAT,
        (parser, value) -> parser.getDoubleValue() == (double) value);
  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 2) {
      throw new KsqlFunctionException("ARRAY_CONTAINS udf should have two input argument. "
          + "Given: " + Arrays.toString(args));
    }
    Object searchValue = args[1];
    if (args[0] instanceof String) {
      return jsonStringArrayContains(searchValue, (String) args[0]);
    } else if (args[0] instanceof Object[]) {
      return ArrayUtil.containsValue(searchValue, (Object[]) args[0]);
    }
    throw new KsqlFunctionException("Invalid type parameters for " + Arrays.toString(args));
  }

  private boolean jsonStringArrayContains(Object searchValue, String jsonArray) {
    JsonToken valueType = getType(searchValue);
    try (JsonParser parser = JSON_FACTORY.createParser(jsonArray)) {
      if (parser.nextToken() != START_ARRAY) {
        return false;
      }

      while (parser.currentToken() != null) {
        JsonToken token = parser.nextToken();
        if (token == null) {
          return searchValue == null;
        }
        if (token == END_ARRAY) {
          return false;
        }
        parser.skipChildren();
        if (valueType == token) {
          final Matcher matcher = matchers.get(valueType);
          if (matcher != null &&  matcher.matches(parser, searchValue)) {
            return true;
          }
        }
      }
    } catch (IOException e) {
      throw new KsqlException("Invalid JSON format: " + jsonArray, e);
    }
    return false;
  }

  /**
   * Returns JsonToken type of the targetValue
   */
  private JsonToken getType(Object searchValue) {
    if (searchValue instanceof Long || searchValue instanceof Integer) {
      return VALUE_NUMBER_INT;
    } else if (searchValue instanceof Double) {
      return VALUE_NUMBER_FLOAT;
    } else if (searchValue instanceof String) {
      return VALUE_STRING;
    } else if (searchValue == null) {
      return VALUE_NULL;
    } else if (searchValue instanceof Boolean) {
      boolean value = (boolean) searchValue;
      return value ? VALUE_TRUE : VALUE_FALSE;
    }
    throw new KsqlFunctionException("Invalid Type for search value " + searchValue);
  }
}
