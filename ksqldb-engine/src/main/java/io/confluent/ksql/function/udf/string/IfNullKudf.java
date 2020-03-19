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

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IfNullKudf implements Kudf {

  private static final Logger LOGGER = LoggerFactory.getLogger(IfNullKudf.class);
  private static final AtomicBoolean FIRST_CALL = new AtomicBoolean(true);

  @Override
  public Object evaluate(final Object... args) {
    if (FIRST_CALL.getAndSet(false)) {
      LOGGER.warn("Use of IFNULL is deprecated and will be removed in a future release. "
          + "Please change your queries to use COALESCE.");
    }

    if (args.length != 2) {
      throw new KsqlFunctionException("IfNull udf should have two input argument.");
    }
    if (args[0] == null) {
      return args[1];
    } else {
      return args[0];
    }
  }
}
