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
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.vertx.core.Vertx;
import org.junit.Before;
import org.junit.Test;

public class ClientImplTest {

  private static final ClientOptions OPTIONS_1 = ClientOptions.create();
  private static final ClientOptions OPTIONS_2 = ClientOptions.create().setUseTls(true);

  private Vertx vertx;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
  }

  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            Client.create(OPTIONS_1)
        )
        .addEqualityGroup(
            Client.create(OPTIONS_1, vertx),
            Client.create(OPTIONS_1, vertx)
        )
        .addEqualityGroup(
            Client.create(OPTIONS_2, vertx)
        )
        .testEquals();
  }

}