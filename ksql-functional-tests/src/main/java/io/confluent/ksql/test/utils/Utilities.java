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

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.HoppingWindowExpression;
import io.confluent.ksql.parser.tree.KsqlWindowExpression;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SessionWindowExpression;
import io.confluent.ksql.parser.tree.TumblingWindowExpression;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.serde.avro.ValueSpecAvroSerdeSupplier;
import io.confluent.ksql.test.serde.json.ValueSpecJsonSerdeSupplier;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class Utilities {

  private Utilities() {

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

  public static Optional<Long> getWindowSize(
      final Query query,
      final MetaStore metaStore) {
    if (query.getWindow().isPresent()) {

      final KsqlWindowExpression ksqlWindowExpression = query
          .getWindow()
          .get().getKsqlWindowExpression();
      if (ksqlWindowExpression instanceof SessionWindowExpression) {
        return Optional.empty();
      }
      final long windowSize = ksqlWindowExpression instanceof TumblingWindowExpression
          ? computeWindowSize(
          ((TumblingWindowExpression) ksqlWindowExpression).getSize(),
          ((TumblingWindowExpression) ksqlWindowExpression).getSizeUnit())
          : computeWindowSize(
              ((HoppingWindowExpression) ksqlWindowExpression).getSize(),
              ((HoppingWindowExpression) ksqlWindowExpression).getSizeUnit());
      return Optional.of(windowSize);
    }
    return Optional.empty();
  }

  private static Long computeWindowSize(final Long windowSize, final TimeUnit timeUnit) {
    switch (timeUnit) {
      case SECONDS:
        return windowSize * 1000;
      case MINUTES:
        return windowSize * 1000 * 60;
      case HOURS:
        return windowSize * 1000 * 60 * 60;
      case DAYS:
        return windowSize * 1000 * 60 * 60 * 24;
      default:
        throw new KsqlException("Invalid window time unit: " + timeUnit);
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

