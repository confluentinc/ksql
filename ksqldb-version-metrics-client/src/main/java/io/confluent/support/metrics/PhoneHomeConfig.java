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

package io.confluent.support.metrics;

import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Use this config for component-specific phone-home clients. It disables writing metrics to Kafka
 * topic and ensures that non-test customer Ids are "anonymous", even if the client sets support
 * topic and customer id in the config. standard phone-home libraries (the difference is a different
 * URI format).
 */
public class PhoneHomeConfig extends BaseSupportConfig {

  private static final Logger log = LogManager
      .getLogger(io.confluent.support.metrics.PhoneHomeConfig.class);

  private static final boolean CONFLUENT_SUPPORT_CUSTOMER_ID_ENABLED_DEFAULT = false;

  /**
   * Will make sure that writing to support metrics topic is disabled and there is no customer
   * endpoint.
   */
  public PhoneHomeConfig(final Properties originals, final String componentId) {
    this(originals, componentId, CONFLUENT_SUPPORT_CUSTOMER_ID_ENABLED_DEFAULT);
  }

  private PhoneHomeConfig(final Properties originals, final String componentId,
      final boolean supportCustomerIdEnabled) {
    super(setupProperties(originals, supportCustomerIdEnabled), getEndpointPath(componentId));
  }

  @SuppressWarnings("checkstyle:FinalParameters")
  private static Properties setupProperties(Properties originals,
      final boolean supportCustomerIdEnabled) {
    if (!supportCustomerIdEnabled) {
      if (originals == null) {
        originals = new Properties();
      }
      if (!isTestUser(getCustomerId(originals))) {
        log.warn("Enforcing customer ID '{}'", CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
        originals.setProperty(CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
            CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
      }
    }
    return originals;
  }


  /**
   * The resulting secure endpoint will be:
   * BaseSupportConfig.CONFLUENT_PHONE_HOME_ENDPOINT_BASE_SECURE/getEndpointPath()/{anon|test}
   * The resulting insecure endpoint will be:
   * BaseSupportConfig.CONFLUENT_PHONE_HOME_ENDPOINT_BASE_INSECURE/getEndpointPath()/{anon|test}
   */
  private static String getEndpointPath(final String componentId) {
    return componentId;
  }

}
