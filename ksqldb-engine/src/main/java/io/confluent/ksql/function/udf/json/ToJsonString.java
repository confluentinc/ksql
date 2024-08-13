/*
 * Copyright 2022 Confluent Inc.
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

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.schema.ksql.SqlTimeTypes;
import io.confluent.ksql.util.BytesUtils;
import io.confluent.ksql.util.BytesUtils.Encoding;
import io.confluent.ksql.util.KsqlConstants;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

@UdfDescription(
    name = "TO_JSON_STRING",
    category = FunctionCategory.JSON,
    description = "Given any ksqlDB type returns the equivalent JSON string.",
    author = KsqlConstants.CONFLUENT_AUTHOR)
public class ToJsonString {

  @Udf
  public <T> String toJsonString(@UdfParameter final T input) {
    return toJson(input);
  }

  private String toJson(final Object input) {
    return UdfJsonMapper.writeValueAsJson(prepare(input));
  }

  private Object prepare(final Object input) {
    if (input instanceof Time) {
      return prepareTime((Time) input);
    } else if (input instanceof Date) {
      return prepareDate((Date) input);
    } else if (input instanceof Timestamp) {
      return prepareTimestamp((Timestamp) input);
    } else if (input instanceof ByteBuffer) {
      return prepareByteBuffer((ByteBuffer) input);
    } else if (input instanceof Struct) {
      return prepareStruct((Struct) input);
    } else if (input instanceof Map) {
      return prepareMap((Map<?, ?>) input);
    } else if (input instanceof List) {
      return prepareList((List<?>) input);
    }
    return input;
  }

  private String prepareTime(final Time time) {
    return SqlTimeTypes.formatTime(time);
  }

  private String prepareDate(final Date date) {
    return SqlTimeTypes.formatDate(date);
  }

  private String prepareTimestamp(final Timestamp timestamp) {
    return SqlTimeTypes.formatTimestamp(timestamp);
  }

  private String prepareByteBuffer(final ByteBuffer bb) {
    return BytesUtils.encode(bb.array(), Encoding.BASE64);
  }

  private Map<String, Object> prepareStruct(final Struct input) {
    // avoids flaky tests by guaranteeing predictable order
    final SortedMap<String, Object> map = new TreeMap<>();
    for (final Field f : input.schema().fields()) {
      map.put(f.name(), prepare(input.get(f)));
    }
    return map;
  }

  private Map<String, Object> prepareMap(final Map<?, ?> input) {
    // avoids flaky tests by guaranteeing predictable order
    final SortedMap<String, Object> map = new TreeMap<>();
    for (final Map.Entry<?, ?> entry : input.entrySet()) {
      map.put(entry.getKey().toString(), prepare(entry.getValue()));
    }
    return map;
  }

  private List<Object> prepareList(final List<?> input) {
    final List<Object> lst = new ArrayList<>(input.size());
    for (Object o : input) {
      lst.add(prepare(o));
    }
    return lst;
  }
}
