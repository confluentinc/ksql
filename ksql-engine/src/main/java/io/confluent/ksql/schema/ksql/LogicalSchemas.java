/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.ksql;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.Type.KsqlType;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;

/**
 * Util class for converting to KSQL's logical schema.
 */
public final class LogicalSchemas {

  public static final Schema BOOLEAN = Schema.OPTIONAL_BOOLEAN_SCHEMA;
  public static final Schema INTEGER = Schema.OPTIONAL_INT32_SCHEMA;
  public static final Schema BIGINT = Schema.OPTIONAL_INT64_SCHEMA;
  public static final Schema DOUBLE = Schema.OPTIONAL_FLOAT64_SCHEMA;
  public static final Schema STRING = Schema.OPTIONAL_STRING_SCHEMA;

  private static final Map<KsqlType, Schema> PRIMITIVE_SQL_TO_LOGICAL = ImmutableMap
      .<KsqlType, Schema>builder()
      .put(KsqlType.STRING, STRING)
      .put(KsqlType.BOOLEAN, BOOLEAN)
      .put(KsqlType.INTEGER, INTEGER)
      .put(KsqlType.BIGINT, BIGINT)
      .put(KsqlType.DOUBLE, DOUBLE)
      .build();

  private LogicalSchemas() {
  }

  /**
   * Get the KSQL logical schema for the corresponding supplied primitive SQL {@code type}.
   *
   * @param type the sql type
   * @return the logical schema.
   */
  public static Schema fromPrimitiveSqlType(final PrimitiveType type) {
    final Schema schema = PRIMITIVE_SQL_TO_LOGICAL.get(type.getKsqlType());
    if (schema == null) {
      throw new IllegalStateException("Unknown primitive type: " + type);
    }

    return schema;
  }
}
