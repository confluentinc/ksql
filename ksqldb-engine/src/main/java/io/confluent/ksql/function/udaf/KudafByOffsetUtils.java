/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.function.udaf;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public final class KudafByOffsetUtils {
  public static final String SEQ_FIELD = "SEQ";
  public static final String VAL_FIELD = "VAL";

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

  public static int compareStructs(final Struct struct1, final Struct struct2) {
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

  private KudafByOffsetUtils() {

  }
}
