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

package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;

public class RoundKudf implements Kudf {

  public static final String NAME = "ROUND";

  @Override
  public Object evaluate(final Object... args) {
    if (args.length != 1 && args.length != 2) {
      throw new KsqlFunctionException("Round udf should have one or two input arguments.");
    }

    if (args[0] == null) {
      return null;
    }
    final Object arg0 = args[0];
    if (!(arg0 instanceof Number)) {
      throw new KsqlFunctionException("Round udf first argument must be a number.");
    }
    final double number = ((Number)arg0).doubleValue();

    if (args.length == 1 || args[1] == null) {
      return Math.round(number);
    }

    final Object arg1 = args[1];
    if (!(arg1 instanceof Integer)) {
      throw new KsqlFunctionException("Round udf second argument must be an integer.");
    }

    final int places = (Integer)arg1;

    final double round = Math.pow(10, places);
    return Math.round(number * round) / round;
  }
}
