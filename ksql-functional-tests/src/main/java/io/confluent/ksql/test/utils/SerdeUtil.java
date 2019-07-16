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

package io.confluent.ksql.test.utils;

import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.serde.avro.ValueSpecAvroSerdeSupplier;
import io.confluent.ksql.test.serde.json.ValueSpecJsonSerdeSupplier;
import io.confluent.ksql.test.serde.kafka.KafkaSerdeSupplier;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import java.util.function.Supplier;

public final class SerdeUtil {

  private SerdeUtil() {
  }

  public static SerdeSupplier<?> getSerdeSupplier(
      final Format format,
      final Supplier<LogicalSchema> schemaSupplier
  ) {
    switch (format) {
      case AVRO:
        return new ValueSpecAvroSerdeSupplier();
      case JSON:
        return new ValueSpecJsonSerdeSupplier();
      case DELIMITED:
        return new StringSerdeSupplier();
      case KAFKA:
        return new KafkaSerdeSupplier(schemaSupplier);
      default:
        throw new InvalidFieldException("format", "unsupported value: " + format);
    }
  }
}
