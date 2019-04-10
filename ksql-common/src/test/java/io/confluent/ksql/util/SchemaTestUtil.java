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

package io.confluent.ksql.util;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;


public final class SchemaTestUtil {

  /**
   * Remove the alias when reading/writing from outside
   */
  public static Schema getSchemaWithNoAlias(final Schema schema) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (final Field field : schema.fields()) {
      final String name = SchemaUtil.getFieldNameWithNoAlias(field);
      schemaBuilder.field(name, field.schema());
    }
    if (schema.isOptional()) {
      schemaBuilder.optional();
    }
    return schemaBuilder.build();
  }
}