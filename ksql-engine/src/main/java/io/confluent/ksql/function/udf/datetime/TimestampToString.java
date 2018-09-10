/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function.udf.datetime;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TimestampToString implements Kudf {

  private DateTimeFormatter threadSafeFormatter;

  @Override
  public Object evaluate(final Object... args) {
    if (args.length < 2) {
      throw new KsqlFunctionException("TimestampToString udf should have at least "
                                    + "two input arguments: date value and format.");
    }

    if (args.length > 3) {
      throw new KsqlFunctionException("TimestampToString udf should have at most "
                                    + "three input arguments: date value, format and zone.");
    }

    try {
      ensureInitialized(args);
      final Timestamp timestamp = new Timestamp((Long) args[0]);
      final ZoneId zoneId =
          (args.length == 3) ? ZoneId.of(args[2].toString()) : ZoneId.systemDefault();
      return timestamp.toInstant()
          .atZone(zoneId)
          .format(threadSafeFormatter);
    } catch (final Exception e) {
      throw new KsqlFunctionException("Exception running TimestampToString(" + args[0] + " , "
          + args[1] + ((args.length == 3) ? (" , " + args[2]) : "") + ") : " + e.getMessage(), e);
    }
  }

  private void ensureInitialized(final Object[] args) {
    if (threadSafeFormatter == null) {
      threadSafeFormatter = DateTimeFormatter.ofPattern(args[1].toString());
    }
  }
}
