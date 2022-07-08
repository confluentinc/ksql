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

package io.confluent.ksql.test.serde.avro;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.ksql.test.serde.ConnectSerdeSupplier;
import org.apache.kafka.connect.data.Schema;

public class ValueSpecAvroSerdeSupplier extends ConnectSerdeSupplier<AvroSchema> {

  public ValueSpecAvroSerdeSupplier() {
    super(AvroConverter::new);
  }

  @Override
  protected Schema fromParsedSchema(final AvroSchema schema) {
    return new AvroData(1).toConnectSchema(schema.rawSchema());
  }

}