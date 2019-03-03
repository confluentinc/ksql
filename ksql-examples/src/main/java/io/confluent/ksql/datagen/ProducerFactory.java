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

package io.confluent.ksql.datagen;

import io.confluent.ksql.datagen.DataGen.Arguments.Format;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;

class ProducerFactory {
  DataGenProducer getProducer(final Format format,
      final String valueDelimiter,
      final String schemaRegistryUrl) {
    switch (format) {
      case AVRO:
        return new AvroProducer(
            new KsqlConfig(
                Collections.singletonMap(
                    KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY,
                    schemaRegistryUrl
                )
            )
        );

      case JSON:
        return new JsonProducer();

      case DELIMITED:
        return new DelimitedProducer(valueDelimiter);

      default:
        throw new IllegalArgumentException("Invalid format in '" + format
            + "'; was expecting one of AVRO, JSON, or DELIMITED%n");
    }
  }
}
