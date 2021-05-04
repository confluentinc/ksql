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
import java.util.Collections;
import org.junit.Test;

public class ConnectorDescriptionImplTest {
  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new ConnectorDescriptionImpl("name", "class", Collections.singletonList("source"), Collections.singletonList("topic"), new ConnectorTypeImpl("SOURCE"), "state"),
            new ConnectorDescriptionImpl("name", "class", Collections.singletonList("source"), Collections.singletonList("topic"), new ConnectorTypeImpl("SOURCE"), "state")
        )
        .addEqualityGroup(
            new ConnectorDescriptionImpl("name2", "class", Collections.singletonList("source"), Collections.singletonList("topic"), new ConnectorTypeImpl("SOURCE"), "state")
        )
        .addEqualityGroup(
            new ConnectorDescriptionImpl("name", "class2", Collections.singletonList("source"), Collections.singletonList("topic"), new ConnectorTypeImpl("SOURCE"), "state")
        )
        .addEqualityGroup(
            new ConnectorDescriptionImpl("name", "class", Collections.emptyList(), Collections.singletonList("topic"), new ConnectorTypeImpl("SOURCE"), "state")
        )
        .addEqualityGroup(
            new ConnectorDescriptionImpl("name", "class", Collections.singletonList("source"), Collections.emptyList(), new ConnectorTypeImpl("SOURCE"), "state")
        )
        .addEqualityGroup(
            new ConnectorDescriptionImpl("name", "class", Collections.singletonList("source"), Collections.singletonList("topic"), new ConnectorTypeImpl("SINK"), "state")
        )
        .addEqualityGroup(
            new ConnectorDescriptionImpl("name", "class", Collections.singletonList("source"), Collections.singletonList("topic"), new ConnectorTypeImpl("SOURCE"), "state2")
        )
        .testEquals();
  }
}
