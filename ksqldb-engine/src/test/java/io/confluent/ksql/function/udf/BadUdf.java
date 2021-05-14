/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.function.udf;

@UdfDescription(name="bad_udf", description = "throws exceptions when called")
@SuppressWarnings({"unused", "MethodMayBeStatic"})
public class BadUdf {

  private int i;

  @Udf(description = "throws")
  public String blowUp(final int arg1) {
    throw new RuntimeException("boom!");
  }

  @Udf(description = "throws if arg is true")
  public int mightThrow(final boolean arg) {
    if (arg) {
      throw new RuntimeException("You asked me to throw...");
    }

    return 0;
  }

  @Udf(description = "returns null every other invocation")
  public String returnNull(final String arg) {
    return i++ % 2 == 0 ? null : arg;
  }
}
