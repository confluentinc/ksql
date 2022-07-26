/*
 * Copyright 2018 Confluent Inc.
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
package io.confluent.ksql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryApplicationId;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;

public class QueryApplicationIdTest {

  private KsqlConfig config;

  @Before
  public void setup() {
    final Properties props = new Properties();
    props.put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "s1");
    config = new KsqlConfig(props);
  }

  @Test
  public void shouldBuildPersistentQueryApplicationId() {
    final String queryAppId =
        QueryApplicationId.build(config, true, new QueryId("q1"));
    assertEquals("_confluent-ksql-s1query_q1", queryAppId);
  }

  @Test
  public void shouldBuildTransientQueryApplicationId() {
    final String queryAppId =
        QueryApplicationId.build(config, false, new QueryId("q1"));
    assertTrue(queryAppId.startsWith("_confluent-ksql-s1transient_q1_"));
  }
}
