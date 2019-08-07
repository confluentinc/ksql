/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import com.google.common.testing.EqualsTester;
import java.util.Optional;
import org.junit.Test;

public class ListTopicsTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    // Note: At the moment location does not take part in equality testing
    new EqualsTester()
        .addEqualityGroup(
            new ListTopics(Optional.of(SOME_LOCATION), true),
            new ListTopics(Optional.of(OTHER_LOCATION), true)
        )
        .addEqualityGroup(
            new ListTopics(Optional.of(SOME_LOCATION), false),
            new ListTopics(Optional.of(OTHER_LOCATION), false)
        )
        .testEquals();
  }
}