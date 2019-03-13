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

public class AllColumnsTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);

  private static final Optional<QualifiedName> SOME_PREFIX = Optional.of(
      QualifiedName.of("Bob")
  );

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new AllColumns(SOME_PREFIX),
            new AllColumns(SOME_PREFIX),
            new AllColumns(Optional.of(SOME_LOCATION), SOME_PREFIX),
            new AllColumns(Optional.of(OTHER_LOCATION), SOME_PREFIX)
        )
        .addEqualityGroup(
            new AllColumns(Optional.empty())
        )
        .testEquals();
  }
}