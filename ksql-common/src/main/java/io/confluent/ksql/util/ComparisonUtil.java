/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

public final class ComparisonUtil {

  private static final Map<Type, Function<Type, Boolean>> TYPE_COMPARISON_COMPATIBILITY
      = ImmutableMap.<Schema.Type, Function<Schema.Type, Boolean>>builder()
      .put(Schema.Type.INT32, SchemaUtil::isNumber)
      .put(Schema.Type.INT64, SchemaUtil::isNumber)
      .put(Schema.Type.FLOAT64, SchemaUtil::isNumber)
      .put(Schema.Type.STRING, type -> type == Schema.Type.STRING)
      .put(Schema.Type.BOOLEAN, type -> type == Schema.Type.BOOLEAN)
      .put(Schema.Type.ARRAY, type -> false)
      .put(Schema.Type.MAP, type -> false)
      .put(Schema.Type.STRUCT, type -> false)
      .build();

  private ComparisonUtil() {

  }

  public static boolean areCompatibleTypesForComparison(final Type leftType, final Type rightType) {
    return TYPE_COMPARISON_COMPATIBILITY.get(leftType).apply(rightType);
  }
}
