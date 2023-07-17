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

package io.confluent.ksql.test.serde.json;

import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.ksql.serde.json.JsonSchemaTranslator;
import io.confluent.ksql.test.serde.ConnectSerdeSupplier;
import org.apache.kafka.connect.data.Schema;

public class ValueSpecJsonSchemaSerdeSupplier extends ConnectSerdeSupplier<JsonSchema> {
  private final JsonSchemaTranslator schemaTranslator;

  public ValueSpecJsonSchemaSerdeSupplier() {
    super(JsonSchemaConverter::new);
    this.schemaTranslator = new JsonSchemaTranslator();
  }

  @Override
  protected Schema fromParsedSchema(final JsonSchema schema) {
    return schemaTranslator.toConnectSchema(schema);
  }
}
