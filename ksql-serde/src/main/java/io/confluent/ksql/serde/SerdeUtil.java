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

package io.confluent.ksql.serde;

import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public final class SerdeUtil {

  private SerdeUtil() {}

  public static boolean shouldUnwrap(
      final Schema schema,
      final KsqlConfig ksqlConfig
  ) {
    if (schema.fields().size() != 1) {
      return false;
    }

    return !ksqlConfig.getBoolean(KsqlConfig.KSQL_WRAP_SINGLE_VALUES);
  }

  public static Schema unwrapSchema(final Schema schema) {
    if (schema.fields().size() != 1) {
      throw new IllegalStateException("Expected only single field");
    }

    final Field onlyField = schema.fields().get(0);
    return onlyField.schema();
  }

  public static Object unwrapStruct(final Struct struct) {
    if (struct.schema().fields().size() != 1) {
      throw new SerializationException("Expected to single field.");
    }

    return struct.get(struct.schema().fields().get(0));
  }
}
