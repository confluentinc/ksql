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
import io.confluent.ksql.serde.delimited.KsqlDelimitedSerializer;
import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.kafka.common.serialization.Serializer;

public class DelimitedProducer extends DataGenProducer {

  private final CSVFormat csvFormat;
  public DelimitedProducer(final String valueDelimiter) {
    super();
    this.csvFormat = CSVFormat.newFormat(valueDelimiter.charAt(0));
  }

  @Override
  protected Serializer<GenericRow> getSerializer(
      final Schema avroSchema,
      final org.apache.kafka.connect.data.Schema kafkaSchema,
      final String topicName
  ) {
    return new KsqlDelimitedSerializer(kafkaSchema, csvFormat);
  }
}
