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

package io.confluent.ksql.schema.persistence;

import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.schema.ksql.KsqlSchema;
import io.confluent.ksql.util.KsqlConfig;
import org.junit.Test;

public class PersistenceSchemasFactoryTest {

  @Test
  public void shouldNPE() {
    new NullPointerTester()
        .setDefault(KsqlSchema.class, mock(KsqlSchema.class))
        .setDefault(KsqlConfig.class, new KsqlConfig(ImmutableMap.of()))
        .testAllPublicStaticMethods(PersistenceSchemasFactory.class);
  }
}