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

package io.confluent.ksql.api.client.impl;

import com.google.common.testing.EqualsTester;
import org.junit.Test;

@SuppressWarnings("UnstableApiUsage")
public class TableInfoImplTest {

  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new TableInfoImpl("name", "topic", "KAFKA", "JSON", true),
            new TableInfoImpl("name", "topic", "KAFKA", "JSON", true)
        )
        .addEqualityGroup(
            new TableInfoImpl("other_name", "topic", "KAFKA", "JSON", true)
        )
        .addEqualityGroup(
            new TableInfoImpl("name", "other_topic", "KAFKA", "JSON", true)
        )
        .addEqualityGroup(
            new TableInfoImpl("name", "other_topic", "AVRO", "JSON", true)
        )
        .addEqualityGroup(
            new TableInfoImpl("name", "topic", "KAFKA", "AVRO", true)
        )
        .addEqualityGroup(
            new TableInfoImpl("name", "topic", "KAFKA", "JSON", false)
        )
        .testEquals();
  }
}