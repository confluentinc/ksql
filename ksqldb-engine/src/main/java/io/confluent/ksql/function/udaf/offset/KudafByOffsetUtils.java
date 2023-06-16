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

package io.confluent.ksql.function.udaf.offset;

import static io.confluent.ksql.serde.connect.ConnectKsqlSchemaTranslator.OPTIONAL_DATE_SCHEMA;
import static io.confluent.ksql.serde.connect.ConnectKsqlSchemaTranslator.OPTIONAL_TIMESTAMP_SCHEMA;
import static io.confluent.ksql.serde.connect.ConnectKsqlSchemaTranslator.OPTIONAL_TIME_SCHEMA;

import java.util.Comparator;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

final class KudafByOffsetUtils {

  static final String SEQ_FIELD = "SEQ";
  public static final String VAL_FIELD = "VAL";

  static final Schema STRUCT_INTEGER = SchemaBuilder.struct().optional()
      .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(VAL_FIELD, Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  static final Schema STRUCT_LONG = SchemaBuilder.struct().optional()
      .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(VAL_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .build();

  static final Schema STRUCT_DOUBLE = SchemaBuilder.struct().optional()
      .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(VAL_FIELD, Schema.OPTIONAL_FLOAT64_SCHEMA)
      .build();

  static final Schema STRUCT_BOOLEAN = SchemaBuilder.struct().optional()
      .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(VAL_FIELD, Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .build();

  static final Schema STRUCT_STRING = SchemaBuilder.struct().optional()
      .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(VAL_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  static final Schema STRUCT_TIMESTAMP = SchemaBuilder.struct().optional()
      .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(VAL_FIELD, OPTIONAL_TIMESTAMP_SCHEMA)
      .build();

  static final Schema STRUCT_TIME = SchemaBuilder.struct().optional()
      .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(VAL_FIELD, OPTIONAL_TIME_SCHEMA)
      .build();

  static final Schema STRUCT_DATE = SchemaBuilder.struct().optional()
      .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(VAL_FIELD, OPTIONAL_DATE_SCHEMA)
      .build();

  static final Schema STRUCT_BYTES = SchemaBuilder.struct().optional()
      .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(VAL_FIELD, Schema.OPTIONAL_BYTES_SCHEMA)
      .build();
  
  static final Comparator<Struct> INTERMEDIATE_STRUCT_COMPARATOR = (struct1, struct2) -> {
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
  };

  static final Comparator<Struct> INTERMEDIATE_STRUCT_COMPARATOR_IGNORE_NULLS =
      (struct1, struct2) -> {
        // Ignore nulls: If one of the structs has a null value, then return the other irrespective
        // of sequence.
        if (struct1.get(VAL_FIELD) == null && struct2.get(VAL_FIELD) == null) {
          return 0;
        } else if (struct1.get(VAL_FIELD) == null) {
          return -1;
        } else if (struct2.get(VAL_FIELD) == null) {
          return 1;
        }

        return INTERMEDIATE_STRUCT_COMPARATOR.compare(struct1, struct2);
      };

  static Schema buildSchema(final Schema schema) {
    return SchemaBuilder.struct().optional()
        .field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
        .field(VAL_FIELD, schema)
        .build();
  }

  static <T> Struct createStruct(final Schema schema, final long sequence, final T val) {
    final Struct struct = new Struct(schema);
    struct.put(SEQ_FIELD, sequence);
    struct.put(VAL_FIELD, val);
    return struct;
  }

  private KudafByOffsetUtils() {
  }
}
