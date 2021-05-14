/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.test.serde.protobuf;

import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.ksql.test.serde.ConnectSerdeSupplier;
import org.apache.kafka.connect.data.Schema;

public class ValueSpecProtobufSerdeSupplier extends ConnectSerdeSupplier<ProtobufSchema> {

  public ValueSpecProtobufSerdeSupplier() {
    super(ProtobufConverter::new);
  }

  @Override
  protected Schema fromParsedSchema(final ProtobufSchema schema) {
    return new ProtobufData(1).toConnectSchema(schema);
  }
}
