/**
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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;

public class TimestampToString implements Kudf {

  private DateFormat dateFormat = null;
  @Override
  public void init() {

  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 2) {
      throw new KsqlFunctionException("TimestampToString udf should have two input argument:"
                                      + " date value and format.");
    }
    try {
      if(dateFormat == null) {
        dateFormat = new SimpleDateFormat(args[1].toString());
      }
      return dateFormat.format(new Date((long)args[0]));
    } catch (Exception e) {
      throw new KsqlFunctionException("Exception running TimestampToString(" + args[0] +" , "
          + args[1] + ") : " + e.getMessage(), e);
    }
  }
}
