/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function.udf.structfieldextractor;

import io.confluent.ksql.function.FunctionUtil;
import io.confluent.ksql.function.udf.Kudf;
import org.apache.kafka.connect.data.Struct;

public class FetchFieldFromStruct implements Kudf {

  public static final String FUNCTION_NAME = "FETCH_FIELD_FROM_STRUCT";

  @Override
  public Object evaluate(final Object... args) {
    FunctionUtil.ensureCorrectArgs(FUNCTION_NAME, args, Struct.class, String.class);
    if (args[0] == null) {
      return null;
    }
    final Struct struct = (Struct) args[0];
    return struct.get((String) args[1]);
  }

}