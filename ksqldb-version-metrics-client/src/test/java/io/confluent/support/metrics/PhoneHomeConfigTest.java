/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.support.metrics;

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import org.junit.Test;

public class PhoneHomeConfigTest {

  private static final String EXPECTED_SECURE_ENDPOINT = "https://version-check.confluent.io";
  private static final String EXPECTED_INSECURE_ENDPOINT = "http://version-check.confluent.io";

  @Test
  public void testProductionEndpoints() {
    Properties overrideProps = new Properties();
    overrideProps.setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG, "c11");

    BaseSupportConfig config = new PhoneHomeConfig(overrideProps, "TestComponent");
    assertEquals(BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT, config.getCustomerId());
    assertEquals(EXPECTED_SECURE_ENDPOINT + "/TestComponent/anon",
                 config.getEndpointHttps());
    assertEquals(EXPECTED_INSECURE_ENDPOINT + "/TestComponent/anon",
                 config.getEndpointHttp());
  }

  @Test
  public void testTestEndpoints() {
    Properties overrideProps = new Properties();
    overrideProps.setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
                              BaseSupportConfig.CONFLUENT_SUPPORT_TEST_ID_DEFAULT);

    BaseSupportConfig config = new PhoneHomeConfig(overrideProps, "TestComponent");

    assertEquals(BaseSupportConfig.CONFLUENT_SUPPORT_TEST_ID_DEFAULT, config.getCustomerId());
    assertEquals(EXPECTED_SECURE_ENDPOINT + "/TestComponent/test",
                 config.getEndpointHttps());
    assertEquals(EXPECTED_INSECURE_ENDPOINT + "/TestComponent/test",
                 config.getEndpointHttp());
  }

  @Test
  public void testInsecureEndpointDisabled() {
    Properties overrideProps = new Properties();
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG, "false");

    BaseSupportConfig config = new PhoneHomeConfig(overrideProps, "TestComponent");
    assertEquals(BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT, config.getCustomerId());
    assertEquals(EXPECTED_SECURE_ENDPOINT + "/TestComponent/anon",
                 config.getEndpointHttps());
    assertEquals("",
                 config.getEndpointHttp());
  }

}
