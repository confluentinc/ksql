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

package io.confluent.ksql.api.client.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import org.junit.Test;

public class CreateConnectorResultImplTest {
  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new CreateConnectorResultImpl("c1", new ConnectorTypeImpl("SOURCE"), ImmutableMap.of("a", "b")),
            new CreateConnectorResultImpl("c1", new ConnectorTypeImpl("SOURCE"), ImmutableMap.of("a", "b"))
        )
        .addEqualityGroup(
            new CreateConnectorResultImpl("c2", new ConnectorTypeImpl("SOURCE"), ImmutableMap.of("a", "b"))
        )
        .addEqualityGroup(
            new CreateConnectorResultImpl("c1", new ConnectorTypeImpl("SINK"), ImmutableMap.of("a", "b"))
        )
        .addEqualityGroup(
            new CreateConnectorResultImpl("c1", new ConnectorTypeImpl("SINK"), ImmutableMap.of("a", "bb"))
        )
        .testEquals();
  }
}
