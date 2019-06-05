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

package io.confluent.ksql.datagen;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.persistence.PersistenceSchema;
import io.confluent.ksql.serde.GenericRowSerDe.GenericRowSerializer;
import io.confluent.ksql.serde.json.KsqlJsonSerializer;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;

public class JsonProducer extends DataGenProducer {

  @Override
  protected Serializer<GenericRow> getSerializer(
      final Schema avroSchema,
      final org.apache.kafka.connect.data.Schema kafkaSchema,
      final String topicName
  ) {
    return new GenericRowSerializer(
        new KsqlJsonSerializer(PersistenceSchema.of((ConnectSchema) kafkaSchema)),
        kafkaSchema
    );
  }
}
