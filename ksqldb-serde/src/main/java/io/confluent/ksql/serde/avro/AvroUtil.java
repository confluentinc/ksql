/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.serde.avro;

import io.confluent.ksql.schema.connect.SchemaWalker;
import io.confluent.ksql.schema.connect.SchemaWalker.Visitor;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

final class AvroUtil {

  private AvroUtil() {
  }

  static Schema throwOnInvalidSchema(final Schema schema) {

    class SchemaValidator implements Visitor<Void, Void> {

      @Override
      public Void visitMap(final Schema schema, final Void key, final Void value) {
        if (schema.keySchema().type() != Type.STRING) {
          throw new KsqlException("Avro only supports MAPs with STRING keys");
        }
        return null;
      }

      @Override
      public Void visitSchema(final Schema schema) {
        return null;
      }
    }

    SchemaWalker.visit(schema, new SchemaValidator());
    return schema;
  }
}
