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

package org.damian.ksql.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

/**
 * This class deliberately has same FCQN as the other Multiply class in the udf-example.jar
 * We do this to demonstrate classloader isolation and how you can have two UDFs with different
 * classes with same FQCNs in different jars - each jar should be given its own classloader with
 * a child first delegation policy
 */
@UdfDescription(name = "multiply2", description = "multiplies 2 numbers shadowed version!")
public class Multiply {

  @Udf(description = "multiply two ints")
  public long multiply(final int v1, final int v2) {
    // Deliberately return wrong value so we can distinguish it from the other multiply udf
    return 1 + v1 * v2;
  }

}
