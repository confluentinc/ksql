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

import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.serde.avro.ValueSpecAvroSerdeSupplier;
import io.confluent.ksql.test.serde.json.ValueSpecJsonSerdeSupplier;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.util.KsqlException;

public final class SerdeUtil {

  private SerdeUtil() {

  }

  @SuppressWarnings("rawtypes")
  public static SerdeSupplier getSerdeSupplier(final Format format) {
    switch (format) {
      case AVRO:
        return new ValueSpecAvroSerdeSupplier();
      case JSON:
        return new ValueSpecJsonSerdeSupplier();
      case DELIMITED:
        return new StringSerdeSupplier();
      default:
        throw new InvalidFieldException("format", "unsupported value: " + format);
    }
  }

  public static SerdeSupplier getSerdeSupplierForKsqlSerdeFactory(
      final KsqlSerdeFactory ksqlSerdeFactory) {
    switch (ksqlSerdeFactory.getFormat()) {
      case AVRO:
        return new ValueSpecAvroSerdeSupplier();
      case JSON:
        return new ValueSpecJsonSerdeSupplier();
      case DELIMITED:
        return
            new StringSerdeSupplier();
      default:
        throw new KsqlException("Unsupported serde: " + ksqlSerdeFactory.getFormat());
    }
  }
}
