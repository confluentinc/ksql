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
 */

package org.damian.ksql.udf;


import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

/**
 * Class used to test UDFs. This is packaged in udf-example.jar
 */
@UdfDescription(name = "multiply", description = "multiplies 2 numbers")
public class Multiply {

  @Udf(description = "multiply two ints")
  public long multiply(final int v1, final int v2) {
    return v1 * v2;
  }

  @Udf(description = "multiply two longs")
  public long multiply(final long v1, final long v2) {
    return v1 * v2;
  }

  @Udf(description = "multiply two doubles")
  public double multiply(final double v1, double v2) {
    return v1 * v2;
  }

}
