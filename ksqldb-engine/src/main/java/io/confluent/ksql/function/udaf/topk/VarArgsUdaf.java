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

package io.confluent.ksql.function.udaf.topk;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.ParameterInfo;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.streams.kstream.Merger;

// JNH: Ok, so "Struct" needs to be something else..."
public class VarArgsUdaf extends BaseAggregateFunction<Object[], Map<String, Long>, String> {
  public static final ObjectMapper INSTANCE;
  private static final ObjectReader OBJECT_READER;

  static {
    INSTANCE = new ObjectMapper()
        .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
        .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    OBJECT_READER = INSTANCE.reader();
  }

  public VarArgsUdaf(
      final int argIndexInValue,
      final SqlType aggregateType,
      final SqlType outputType) {
    super(
        VarArgsUdafFactory.FUNCTION_NAME,
        argIndexInValue,
        () -> null,
        aggregateType,
        outputType,
        Collections.singletonList(//JNH: Not sure how this is used yet.
            new ParameterInfo("key", ParamTypes.LONG, "", false)),
        "description");
  }

  @Override
  public boolean isVariadic() {
    return super.isVariadic();
  }

  @Override
  public Map<String, Long> aggregate(final Object[] currentValue,
      final Map<String, Long> aggregateValue) {

    HashMap<String, Long> map = new HashMap<>();

    if (aggregateValue != null) {
      map = new HashMap<>(aggregateValue);
    }

    for (Object o : currentValue) {
      final String className = o.getClass().getName();
      if (map.containsKey(className)) {
        Long value = map.get(className);
        map.put(className, ++value);
      } else {
        map.put(className, 1L);
      }
    }

    return map;
  }

  @Override
  public Merger<GenericKey, Map<String, Long>> getMerger() {
    return (aggKey, aggOne, aggTwo) -> {
      // JNH: Shortcut to see something work.
      return aggTwo;
    };
  }

  @Override
  public Function<Map<String, Long>, String> getResultMapper() {
    return struct -> {
      try {
        System.out.println("Returning " + INSTANCE.writeValueAsString(struct));
        return INSTANCE.writeValueAsString(struct);
      } catch (JsonProcessingException e) {
        return "";  // JNH: In case of errror, return an empty string.
      }
    };
  }
}
