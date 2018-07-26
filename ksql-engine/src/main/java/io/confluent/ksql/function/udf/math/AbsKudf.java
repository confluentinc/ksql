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

package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.UdfUtil;
import io.confluent.ksql.function.udf.Kudf;

public class AbsKudf implements Kudf {
  public static final String NAME = "ABS";

  @Override
  public Object evaluate(final Object... args) {
    UdfUtil.ensureCorrectArgs(NAME, args, Number.class);
    if (args[0] == null) {
      return null;
    }
    return Math.abs(((Number) args[0]).doubleValue());
  }
}
