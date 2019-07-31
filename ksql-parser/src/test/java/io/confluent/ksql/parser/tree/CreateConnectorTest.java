/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class CreateConnectorTest {

  private static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  private static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);

  private static final String NAME = "foo";
  private static final String OTHER_NAME = "bar";

  private static final Map<String, Literal> CONFIG = ImmutableMap.of("foo", new StringLiteral("bar"));
  private static final Map<String, Literal> OTHER_CONFIG = ImmutableMap.of("foo", new StringLiteral("baz"));

  @Test
  public void testEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new CreateConnector(Optional.of(SOME_LOCATION), NAME, CONFIG, CreateConnector.Type.SOURCE),
            new CreateConnector(Optional.of(OTHER_LOCATION), NAME, CONFIG, CreateConnector.Type.SOURCE),
            new CreateConnector(NAME, CONFIG, CreateConnector.Type.SOURCE)
        )
        .addEqualityGroup(
            new CreateConnector(OTHER_NAME, CONFIG, CreateConnector.Type.SOURCE)
        )
        .addEqualityGroup(
            new CreateConnector(NAME, OTHER_CONFIG, CreateConnector.Type.SOURCE)
        )
        .addEqualityGroup(
            new CreateConnector(NAME, CONFIG, CreateConnector.Type.SINK)
        )
        .testEquals();
  }

}