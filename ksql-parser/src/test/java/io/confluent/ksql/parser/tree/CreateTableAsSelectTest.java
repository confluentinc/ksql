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

import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import java.util.Optional;
import org.junit.Test;

public class CreateTableAsSelectTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final QualifiedName SOME_NAME = QualifiedName.of("bob");
  private static final CreateSourceAsProperties SOME_PROPS = CreateSourceAsProperties.from(
      ImmutableMap.of("KAFKA_TOPIC", new StringLiteral("value"))
  );
  private static final Query SOME_QUERY = mock(Query.class);

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new CreateTableAsSelect(SOME_NAME, SOME_QUERY, true, SOME_PROPS),
            new CreateTableAsSelect(Optional.of(SOME_LOCATION), SOME_NAME, SOME_QUERY, true, SOME_PROPS)
        )
        .addEqualityGroup(
            new CreateTableAsSelect(QualifiedName.of("diff"), SOME_QUERY, true, SOME_PROPS)
        )
        .addEqualityGroup(
            new CreateTableAsSelect(SOME_NAME, mock(Query.class), true, SOME_PROPS)
        )
        .addEqualityGroup(
            new CreateTableAsSelect(SOME_NAME, SOME_QUERY, false, SOME_PROPS)
        )
        .addEqualityGroup(
            new CreateTableAsSelect(SOME_NAME, SOME_QUERY, true, CreateSourceAsProperties.none())
        )
        .addEqualityGroup(
            new CreateStreamAsSelect(SOME_NAME, SOME_QUERY, true, CreateSourceAsProperties.none(), Optional.empty())
        )
        .testEquals();
  }
}