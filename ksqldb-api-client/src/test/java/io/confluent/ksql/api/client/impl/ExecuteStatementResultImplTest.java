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

package io.confluent.ksql.api.client.impl;

import com.google.common.testing.EqualsTester;
import java.util.Optional;
import org.junit.Test;

public class ExecuteStatementResultImplTest {

  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new ExecuteStatementResultImpl(Optional.of("CSAS_0")),
            new ExecuteStatementResultImpl(Optional.of("CSAS_0"))
        )
        .addEqualityGroup(
            new ExecuteStatementResultImpl(Optional.empty()),
            new ExecuteStatementResultImpl(Optional.empty())
        )
        .addEqualityGroup(
            new ExecuteStatementResultImpl(Optional.of("CSAS_1"))
        )
        .testEquals();
  }

}