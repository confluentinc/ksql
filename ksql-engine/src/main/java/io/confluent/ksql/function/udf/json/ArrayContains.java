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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.util.List;

@UdfDescription(name = "ARRAYCONTAINS", description = ArrayContains.DESCRIPTION)
public class ArrayContains {

  private static final JsonFactory JSON_FACTORY =
      new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);
  static final String DESCRIPTION = "Checks if the array given as argument contains the "
      + "specified value. The array can be a json array or a sql array.";

  interface Matcher {
    boolean matches(JsonParser parser, Object searchValue) throws IOException;
  }

  @Udf
  public boolean arrayContains(@UdfParameter final String jsonArray,
                               @UdfParameter final String value) {
    if (jsonArray == null || value == null) {
      return false;
    }
    return jsonStringArrayContains(jsonArray, value, (parser, v) -> {
      return parser.getText().equals(v); });
  }

  @Udf
  public boolean arrayContains(@UdfParameter final String jsonArray,
                               @UdfParameter final Boolean value) {
    if (jsonArray == null || value == null) {
      return false;
    }
    return jsonStringArrayContains(jsonArray, value, (parser, v) -> {
      return parser.getBooleanValue() == (Boolean)v; });
  }

  @Udf
  public boolean arrayContains(@UdfParameter final String jsonArray,
                               @UdfParameter final Integer value) {
    if (jsonArray == null || value == null) {
      return false;
    }
    return jsonStringArrayContains(jsonArray, value, (parser, v) -> {
      return parser.getIntValue() == (Integer)v; });
  }

  @Udf
  public boolean arrayContains(@UdfParameter final String jsonArray,
                               @UdfParameter final Long value) {
    if (jsonArray == null || value == null) {
      return false;
    }
    return jsonStringArrayContains(jsonArray, value, (parser, v) -> {
      return parser.getLongValue() == (Long)v; });
  }

  @Udf
  public boolean arrayContains(@UdfParameter final String jsonArray,
                               @UdfParameter final Double value) {
    if (jsonArray == null || value == null) {
      return false;
    }
    return jsonStringArrayContains(jsonArray, value, (parser, v) -> {
      return parser.getDoubleValue() == (Double)v; });
  }

  @Udf
  public boolean arrayContains(@UdfParameter final List<?> array,
                               @UdfParameter final Integer value) {
    if (array == null || value == null) {
      return false;
    }
    return array.contains(value);
  }

  @Udf
  public boolean arrayContains(@UdfParameter final List<?> array, @UdfParameter final Long value) {
    if (array == null || value == null) {
      return false;
    }
    return array.contains(value);
  }

  @Udf
  public boolean arrayContains(@UdfParameter final List<?> array,
                               @UdfParameter final Double value) {
    if (array == null || value == null) {
      return false;
    }
    return array.contains(value);
  }

  @Udf
  public boolean arrayContains(@UdfParameter final List<?> array,
                               @UdfParameter final String value) {
    if (array == null || value == null) {
      return false;
    }
    return array.contains(value);
  }

  @Udf
  public boolean arrayContains(@UdfParameter final List<?> array,
                               @UdfParameter final Boolean value) {
    if (array == null || value == null) {
      return false;
    }
    return array.contains(value);
  }

  private boolean jsonStringArrayContains(final String jsonArray,
                                          final Object searchValue,
                                          final Matcher matcher) {

    final JsonToken valueType = getType(searchValue);
    try (JsonParser parser = JSON_FACTORY.createParser(jsonArray)) {
      if (parser.nextToken() != START_ARRAY) {
        return false;
      }

      while (parser.currentToken() != null) {
        final JsonToken token = parser.nextToken();
        if (token == null) {
          return searchValue == null;
        }
        if (token == END_ARRAY) {
          return false;
        }
        parser.skipChildren();
        if (valueType == token) {
          if (matcher != null &&  matcher.matches(parser, searchValue)) {
            return true;
          }
        }
      }
    } catch (final IOException e) {
      throw new KsqlException("Invalid JSON format: " + jsonArray, e);
    }
    return false;
  }

  /**
   * Returns JsonToken type of the targetValue
   */
  private JsonToken getType(final Object searchValue) {
    if (searchValue instanceof Long || searchValue instanceof Integer) {
      return VALUE_NUMBER_INT;
    } else if (searchValue instanceof Double) {
      return VALUE_NUMBER_FLOAT;
    } else if (searchValue instanceof String) {
      return VALUE_STRING;
    } else if (searchValue == null) {
      return VALUE_NULL;
    } else if (searchValue instanceof Boolean) {
      final boolean value = (boolean) searchValue;
      return value ? VALUE_TRUE : VALUE_FALSE;
    }
    throw new KsqlFunctionException("Invalid Type for search value " + searchValue);
  }
}
