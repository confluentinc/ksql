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

package org.damian.ksql.udf;

import java.util.Map;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

/**
 * Class used to test UDFs. This is packaged in udf-example.jar
 */
@UdfDescription(name = "tostring", description = "converts things to a string")
public class ToString {

  @Udf
  public String fromString(final String value) {return value;}

  @Udf
  public String fromMap(final Map<String, Integer> value) {
    return value.toString();
  }

  @Udf
  public String fromInt(final Integer value) {
    return value.toString();
  }

  @Udf
  public String fromLong(final Long value) {
    return value.toString();
  }

}
