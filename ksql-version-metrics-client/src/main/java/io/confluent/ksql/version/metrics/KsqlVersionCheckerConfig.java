/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.confluent.ksql.version.metrics;

import java.util.Properties;

import io.confluent.support.metrics.BaseSupportConfig;


public class KsqlVersionCheckerConfig extends BaseSupportConfig {

  public static final String KSQL_VERSION_CHECK_INSECURE_ENDPOINT =
      "http://version-check.confluent.io/ksql/anon";

  public static final String KSQL_VERSION_CHECK_INSECURE_TEST_ENDPOINT =
      "http://version-check.confluent.io/ksql/test";

  public static final String KSQL_VERSION_CHECK_SECURE_ENDPOINT =
      "https://version-check.confluent.io/ksql/anon";
  public static final String KSQL_VERSION_CHECK_SECURE_TEST_ENDPOINT =
      "https://version-check.confluent.io/ksql/test";

  public KsqlVersionCheckerConfig(Properties originals) {
    super(setupProperties(originals));
  }


  private static Properties setupProperties(Properties originals){
    //disable publish to topic
    if (originals == null) {
      originals = new Properties();
    }
    originals.setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "");
    return originals;
  }

  @Override
  protected String getAnonymousEndpoint(boolean secure) {
    if (secure){
      return KSQL_VERSION_CHECK_SECURE_ENDPOINT;
    } else {
      return KSQL_VERSION_CHECK_INSECURE_ENDPOINT;
    }
  }

  @Override
  protected String getTestEndpoint(boolean secure) {
    if (secure){
      return KSQL_VERSION_CHECK_SECURE_TEST_ENDPOINT;
    } else {
      return KSQL_VERSION_CHECK_INSECURE_TEST_ENDPOINT;
    }
  }

  @Override
  protected String getCustomerEndpoint(boolean secure) {
    return getAnonymousEndpoint(secure);
  }

}
