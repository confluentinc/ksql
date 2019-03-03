/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.serde.delimited;

import static io.confluent.ksql.processing.log.ProcessingLoggerUtil.join;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.processing.log.ProcessingLogContext;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.util.SerdeUtils;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.commons.csv.CSVFormat;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;



public class KsqlDelimitedTopicSerDe extends KsqlTopicSerDe {

  private final CSVFormat csvFormat;

  public KsqlDelimitedTopicSerDe(final String delimiter) {
    super(DataSource.DataSourceSerDe.DELIMITED);

    if (delimiter.length() == 1) {
      this.csvFormat = CSVFormat.DEFAULT.withDelimiter(delimiter.charAt(0));
    } else {
      throw new KsqlException("Only single characters are supported for VALUE_DELIMITER.");
    }
  }

  @Override
  public Serde<GenericRow> getGenericRowSerde(
      final Schema schema,
      final KsqlConfig ksqlConfig,
      final boolean isInternal,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext) {
    final Map<String, Object> serdeProps = new HashMap<>();

    final Serializer<GenericRow> genericRowSerializer = new KsqlDelimitedSerializer(schema, csvFormat);
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KsqlDelimitedDeserializer(
        schema,
        csvFormat,
        processingLogContext.getLoggerFactory().getLogger(
            join(loggerNamePrefix, SerdeUtils.DESERIALIZER_LOGGER_NAME)),
        processingLogContext
    );

    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }
}
