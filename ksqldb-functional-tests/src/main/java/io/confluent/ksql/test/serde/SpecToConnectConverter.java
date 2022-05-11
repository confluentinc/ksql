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

package io.confluent.ksql.test.serde;

import io.confluent.ksql.serde.SerdeUtils;
import io.confluent.ksql.test.TestFrameworkException;
import io.confluent.ksql.util.BytesUtils;
import io.confluent.ksql.util.BytesUtils.Encoding;
import io.confluent.ksql.util.DecimalUtil;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

public final class SpecToConnectConverter {

  private SpecToConnectConverter() {
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  public static Object specToConnect(final Object spec, final Schema schema) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    if (spec == null) {
      return null;
    }

    switch (schema.type()) {
      case INT32:
        final Integer intVal = Integer.valueOf(spec.toString());
        if (Time.LOGICAL_NAME.equals(schema.name())) {
          return new java.sql.Time(intVal);
        }
        if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name())) {
          return SerdeUtils.getDateFromEpochDays(intVal);
        }
        return intVal;
      case INT64:
        final Long longVal = Long.valueOf(spec.toString());
        if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
          return new java.sql.Timestamp(longVal);
        }
        return longVal;
      case FLOAT32:
        return Float.valueOf(spec.toString());
      case FLOAT64:
        return Double.valueOf(spec.toString());
      case BOOLEAN:
        return Boolean.valueOf(spec.toString());
      case STRING:
        return spec.toString();
      case ARRAY:
        return ((List<?>) spec)
            .stream()
            .map(el -> specToConnect(el, schema.valueSchema()))
            .collect(Collectors.toList());
      case MAP:
        return ((Map<?, ?>) spec)
            .entrySet()
            .stream()
            // cannot use Collectors.toMap due to JDK bug:
            // https://bugs.openjdk.java.net/browse/JDK-8148463
            .collect(
                HashMap::new,
                ((map, v) -> map.put(
                    specToConnect(v.getKey(), schema.keySchema()),
                    specToConnect(v.getValue(), schema.valueSchema()))),
                HashMap::putAll
            );
      case STRUCT:
        final Map<String, String> caseInsensitiveFieldMap = schema.fields()
            .stream()
            .collect(Collectors.toMap(
                f -> f.name().toUpperCase(),
                Field::name
            ));

        final Struct struct = new Struct(schema);
        ((Map<?, ?>) spec)
            .forEach((key, value) -> {
              final String realKey = caseInsensitiveFieldMap.get(key.toString().toUpperCase());
              if (realKey != null) {
                struct.put(realKey, specToConnect(value, schema.field(realKey).schema()));
              }
            });
        return struct;
      case BYTES:
        if (DecimalUtil.isDecimal(schema)) {
          if (spec instanceof BigDecimal) {
            return DecimalUtil.ensureFit((BigDecimal) spec, schema);
          }

          if (spec instanceof String) {
            // Supported for legacy reasons...
            return DecimalUtil.cast(
                (String) spec,
                DecimalUtil.precision(schema),
                DecimalUtil.scale(schema));
          }

          throw new TestFrameworkException("DECIMAL type requires JSON number in test data");
        } else if (spec instanceof  String) {
          // covers PROTOBUF_NOSR
          return BytesUtils.decode((String) spec, Encoding.BASE64);
        } else {
          return spec;
        }
      default:
        throw new RuntimeException(
            "This test does not support the data type yet: " + schema.type());
    }
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  public static Object connectToSpec(
      final Object data,
      final Schema schema,
      final boolean toUpper
  ) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    if (data == null) {
      return null;
    }

    switch (schema.type()) {
      case INT64:
        if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
          return Timestamp.fromLogical(schema, (Date) data);
        }
        return data;
      case INT32:
        if (Time.LOGICAL_NAME.equals(schema.name())) {
          return Time.fromLogical(schema, (Date) data);
        }
        if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name())) {
          return org.apache.kafka.connect.data.Date.fromLogical(schema, (Date) data);
        }
        return data;
      case FLOAT32:
      case FLOAT64:
      case BOOLEAN:
        return data;
      case STRING:
        return data.toString();
      case ARRAY:
        return ((List<?>) data)
            .stream()
            .map(v -> connectToSpec(v, schema.valueSchema(), toUpper))
            .collect(Collectors.toList());
      case MAP:
        final Map<String, Object> map = new HashMap<>();
        ((Map<?, ?>) data)
            .forEach((k, v) -> map.put(
                k.toString(),
                connectToSpec(v, schema.valueSchema(), toUpper)));
        return map;
      case STRUCT:
        final Map<String, Object> recordSpec = new HashMap<>();
        schema.fields()
            .forEach(f -> recordSpec.put(
                toUpper ? f.name().toUpperCase() : f.name(),
                connectToSpec(((Struct) data).get(f.name()), f.schema(), toUpper)));
        return recordSpec;
      case BYTES:
        if (DecimalUtil.isDecimal(schema)) {
          if (data instanceof BigDecimal) {
            return data;
          }
          throw new RuntimeException("Unexpected BYTES type " + schema.name());
        } else {
          if (data instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) data);
          } else {
            return data;
          }
        }
      default:
        throw new RuntimeException("Test cannot handle data of type: " + schema.type());
    }
  }
}
