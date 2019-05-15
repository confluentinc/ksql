/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.serde.json;

import com.google.errorprone.annotations.Immutable;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeUtil;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

@Immutable
public class KsqlJsonSerdeFactory extends KsqlSerdeFactory {

  public KsqlJsonSerdeFactory() {
    super(Format.JSON);
  }

  @Override
  protected Serializer<Struct> createSerializer(
      final Schema logicalSchema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    final boolean unwrap = SerdeUtil.shouldUnwrap(logicalSchema, ksqlConfig);

    final Schema physicalSchema = unwrap
        ? SerdeUtil.unwrapSchema(logicalSchema)
        : logicalSchema;

    final Function<Struct, Object> preprocessor = unwrap
        ? SerdeUtil::unwrapStruct
        : s -> s;

    final Serializer<Struct> serializer = new KsqlJsonSerializer(physicalSchema, preprocessor);
    serializer.configure(Collections.emptyMap(), false);
    return serializer;
  }

  @Override
  protected Deserializer<Struct> createDeserializer(
      final Schema logicalSchema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final ProcessingLogger processingLogger
  ) {
    final Deserializer<Struct> deserializer =
        new KsqlJsonDeserializer(logicalSchema, processingLogger);

    deserializer.configure(Collections.emptyMap(), false);

    return deserializer;
  }
}
