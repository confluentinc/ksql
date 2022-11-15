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

package io.confluent.ksql.execution.ddl.commands;

import static org.mockito.Mockito.mock;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class KsqlTopicTest {

  @Mock
  private KeyFormat keyFormat;
  @Mock
  private ValueFormat valueFormat;

  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new KsqlTopic("name", keyFormat, valueFormat),
            new KsqlTopic("name", keyFormat, valueFormat)
        )
        .addEqualityGroup(
            new KsqlTopic("diff", keyFormat, valueFormat)
        )
        .addEqualityGroup(
            new KsqlTopic("name", mock(KeyFormat.class), valueFormat)
        )
        .addEqualityGroup(
            new KsqlTopic("name", keyFormat, mock(ValueFormat.class))
        )
        .testEquals();
  }
}