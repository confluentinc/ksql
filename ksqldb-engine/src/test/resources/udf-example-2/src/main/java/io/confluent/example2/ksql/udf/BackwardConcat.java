/*
 * Copyright 2020 Confluent Inc.
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

package org.example2.ksql.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

/**
 * Class used to test UDFs. This is packaged in udf-example-2.jar
 */
@UdfDescription(name = "backward_concat", description = "Concats two strings in reverse")
public class BackwardConcat {

  @Udf(description = "Concats two strings in reverse")
  public String backwardConcat(final String str1, final String str2) {
    return str2 + str1;
  }
}
