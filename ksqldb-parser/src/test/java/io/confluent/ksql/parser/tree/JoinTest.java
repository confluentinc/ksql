/*
 * Copyright 2021 Confluent Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JoinTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final Relation RELATION_0 = new Table(SourceName.of("bob"));
  private static final Relation RELATION_1 = new Table(SourceName.of("pete"));

  @Mock
  private JoinedSource someSource;
  @Mock
  private JoinedSource otherSource;

  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new Join(Optional.empty(), RELATION_0, ImmutableList.of(someSource)),
            new Join(Optional.of(new NodeLocation(0, 0)), RELATION_0, ImmutableList.of(someSource)),
            new Join(Optional.of(new NodeLocation(1, 1)), RELATION_0, ImmutableList.of(someSource))
        )
        .addEqualityGroup(
            new Join(Optional.empty(), RELATION_1, ImmutableList.of(someSource))
        )
        .addEqualityGroup(
            new Join(Optional.empty(), RELATION_1, ImmutableList.of(otherSource))
        )
        .testEquals();
  }

}
