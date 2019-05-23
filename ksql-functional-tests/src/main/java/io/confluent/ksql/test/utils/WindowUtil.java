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

import io.confluent.ksql.parser.tree.HoppingWindowExpression;
import io.confluent.ksql.parser.tree.KsqlWindowExpression;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SessionWindowExpression;
import io.confluent.ksql.parser.tree.TumblingWindowExpression;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class WindowUtil {

  private WindowUtil() {}

  public static Optional<Long> getWindowSize(final Query query) {
    if (!query.getWindow().isPresent()) {
      return Optional.empty();
    }
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

  private static Long computeWindowSize(final Long windowSize, final TimeUnit timeUnit) {
    return TimeUnit.MILLISECONDS.convert(windowSize, timeUnit);
  }
}
