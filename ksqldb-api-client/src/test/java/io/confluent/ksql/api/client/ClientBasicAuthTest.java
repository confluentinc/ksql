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

package io.confluent.ksql.api.client;

import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.test.util.TestBasicJaasConfig;
import java.util.Map;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientBasicAuthTest extends ClientTest {

  protected static final Logger log = LoggerFactory.getLogger(ClientBasicAuthTest.class);

  private static final String PROPS_JAAS_REALM = "KsqlServer-Props";
  private static final String KSQL_RESOURCE = "ksql-user";
  private static final String USER_WITH_ACCESS = "harry";
  private static final String USER_WITH_ACCESS_PWD = "changeme";

  @ClassRule
  public static final TestBasicJaasConfig JAAS_CONFIG = TestBasicJaasConfig
      .builder(PROPS_JAAS_REALM)
      .addUser(USER_WITH_ACCESS, USER_WITH_ACCESS_PWD, KSQL_RESOURCE)
      .build();

  @Override
  protected KsqlRestConfig createServerConfig() {
    KsqlRestConfig config = super.createServerConfig();
    Map<String, Object> origs = config.originals();
    origs.put(
        KsqlRestConfig.AUTHENTICATION_METHOD_CONFIG,
        KsqlRestConfig.AUTHENTICATION_METHOD_BASIC);
    origs.put(
        KsqlRestConfig.AUTHENTICATION_REALM_CONFIG,
        PROPS_JAAS_REALM
    );
    origs.put(
        KsqlRestConfig.AUTHENTICATION_ROLES_CONFIG,
        KSQL_RESOURCE
    );
    return new KsqlRestConfig(origs);
  }

  @Override
  protected ClientOptions createJavaClientOptions() {
    return super.createJavaClientOptions()
        .setBasicAuthCredentials(USER_WITH_ACCESS, USER_WITH_ACCESS_PWD);
  }

}
