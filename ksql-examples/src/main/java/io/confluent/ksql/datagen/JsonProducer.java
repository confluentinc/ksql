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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactory;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Serializer;

public class JsonProducer extends DataGenProducer {

  @Override
  protected Serializer<GenericRow> getSerializer(
      final Schema avroSchema,
      final org.apache.kafka.connect.data.Schema kafkaSchema,
      final String topicName
  ) {
    return GenericRowSerDe.from(
        new KsqlJsonSerdeFactory(),
        PhysicalSchema.from(LogicalSchema.of(kafkaSchema), SerdeOption.none()),
        new KsqlConfig(ImmutableMap.of()),
        () -> null,
        "",
        NoopProcessingLogContext.INSTANCE
    ).serializer();
  }
}
