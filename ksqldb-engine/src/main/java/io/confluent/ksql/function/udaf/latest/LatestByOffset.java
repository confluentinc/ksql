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

package io.confluent.ksql.function.udaf.latest;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(
    name = "LATEST_BY_OFFSET",
    description = LatestByOffset.DESCRIPTION
)
public final class LatestByOffset {

  static final String DESCRIPTION =
      "This function returns the most recent value for the column, computed by offset.";

  private LatestByOffset() {
  }

  static final String SEQ_FIELD = "SEQ";
  static final String VAL_FIELD = "VAL";

  public static final Schema STRUCT_INTEGER = SchemaBuilder.struct().optional()
      .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(VAL_FIELD, Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  public static final Schema STRUCT_LONG = SchemaBuilder.struct().optional()
      .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(VAL_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .build();

  public static final Schema STRUCT_DOUBLE = SchemaBuilder.struct().optional()
      .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(VAL_FIELD, Schema.OPTIONAL_FLOAT64_SCHEMA)
      .build();

  public static final Schema STRUCT_BOOLEAN = SchemaBuilder.struct().optional()
      .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(VAL_FIELD, Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .build();

  public static final Schema STRUCT_STRING = SchemaBuilder.struct().optional()
      .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(VAL_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  static AtomicLong sequence = new AtomicLong();

  @UdafFactory(description = "return the latest value of an integer column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL INT>")
  public static Udaf<Integer, Struct, Integer> latestInteger() {
    return latest(STRUCT_INTEGER);
  }

  @UdafFactory(description = "return the latest value of an big integer column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL BIGINT>")
  public static Udaf<Long, Struct, Long> latestLong() {
    return latest(STRUCT_LONG);
  }

  @UdafFactory(description = "return the latest value of a double column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL DOUBLE>")
  public static Udaf<Double, Struct, Double> latestDouble() {
    return latest(STRUCT_DOUBLE);
  }

  @UdafFactory(description = "return the latest value of a boolean column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL BOOLEAN>")
  public static Udaf<Boolean, Struct, Boolean> latestBoolean() {
    return latest(STRUCT_BOOLEAN);
  }

  @UdafFactory(description = "return the latest value of a string column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL STRING>")
  public static Udaf<String, Struct, String> latestString() {
    return latest(STRUCT_STRING);
  }

  static <T> Struct createStruct(final Schema schema, final T val) {
    final Struct struct = new Struct(schema);
    struct.put(SEQ_FIELD, generateSequence());
    struct.put(VAL_FIELD, val);
    return struct;
  }

  private static long generateSequence() {
    return sequence.getAndIncrement();
  }

  private static int compareStructs(final Struct struct1, final Struct struct2) {
    // Deal with overflow - we assume if one is positive and the other negative then the sequence
    // has overflowed - in which case the latest is the one with the smallest sequence
    final long sequence1 = struct1.getInt64(SEQ_FIELD);
    final long sequence2 = struct2.getInt64(SEQ_FIELD);
    if (sequence1 < 0 && sequence2 >= 0) {
      return 1;
    } else if (sequence2 < 0 && sequence1 >= 0) {
      return -1;
    } else {
      return Long.compare(sequence1, sequence2);
    }
  }

  @UdafFactory(description = "Latest by offset")
  static <T> Udaf<T, Struct, T> latest(final Schema structSchema) {
    return new Udaf<T, Struct, T>() {

      @Override
      public Struct initialize() {
        return createStruct(structSchema, null);
      }

      @Override
      public Struct aggregate(final T current, final Struct aggregate) {
        if (current == null) {
          return aggregate;
        } else {
          return createStruct(structSchema, current);
        }
      }

      @Override
      public Struct merge(final Struct aggOne, final Struct aggTwo) {
        // When merging we need some way of evaluating the "latest' one.
        // We do this by keeping track of the sequence of when it was originally processed
        if (compareStructs(aggOne, aggTwo) >= 0) {
          return aggOne;
        } else {
          return aggTwo;
        }
      }

      @Override
      @SuppressWarnings("unchecked")
      public T map(final Struct agg) {
        return (T) agg.get(VAL_FIELD);
      }
    };
  }

}
