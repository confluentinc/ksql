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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import java.util.List;

@UdtfDescription(name = "throwing_udtf", description = "test UDTF that throws if param is true")
@SuppressWarnings({"unused", "MethodMayBeStatic"})
public class ThrowingUdtf {

  @Udtf
  public List<Boolean> throwIfTrue(final boolean shouldThrow) {
    if (shouldThrow) {
      throw new RuntimeException("You asked me to throw...");
    }
    return ImmutableList.of(shouldThrow);
  }
}
