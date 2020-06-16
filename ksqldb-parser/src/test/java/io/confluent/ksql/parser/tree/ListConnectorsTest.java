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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.tree.ListConnectors.Scope;
import java.util.Optional;
import org.junit.Test;

public class ListConnectorsTest {

  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new ListConnectors(Optional.empty(), Scope.ALL),
            new ListConnectors(Optional.of(new NodeLocation(1, 1)), Scope.ALL)
        )
        .addEqualityGroup(
            new ListConnectors(Optional.empty(), Scope.SOURCE)
        )
        .testEquals();
  }

}