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

package io.confluent.ksql.ddl.commands;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.execution.ddl.commands.DropTypeCommand;
import io.confluent.ksql.parser.DropType;
import java.util.Optional;
import org.junit.Test;

public class DropTypeFactoryTest {
  private static final String SOME_TYPE_NAME = "some_type";

  private final DropTypeFactory factory = new DropTypeFactory();

  @Test
  public void shouldCreateDropType() {
    // Given:
    final DropType dropType = new DropType(Optional.empty(), SOME_TYPE_NAME);

    // When:
    final DropTypeCommand cmd = factory.create(dropType);

    // Then:
    assertThat(cmd.getTypeName(), equalTo(SOME_TYPE_NAME));
  }
}
