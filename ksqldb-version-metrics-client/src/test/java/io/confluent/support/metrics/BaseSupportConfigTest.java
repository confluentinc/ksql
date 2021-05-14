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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.support.metrics.utils.CustomerIdExamples;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.Test;

public class BaseSupportConfigTest {

  @Test
  public void testValidCustomer() {
    for (String validId : CustomerIdExamples.VALID_CUSTOMER_IDS) {
      assertTrue(validId + " is an invalid customer identifier",
          BaseSupportConfig.isConfluentCustomer(validId));
    }
  }

  @Test
  public void testValidNewCustomer() {
    String[] validNewCustomerIds = Stream.concat(
        CustomerIdExamples.VALID_CASE_SENSISTIVE_NEW_CUSTOMER_IDS.stream(),
        CustomerIdExamples.VALID_CASE_INSENSISTIVE_NEW_CUSTOMER_IDS.stream()).
        toArray(String[]::new);
    for (String validId : validNewCustomerIds) {
      assertTrue(validId + " is an invalid new customer identifier",
          BaseSupportConfig.isConfluentCustomer(validId));
    }
  }

  @Test
  public void testInvalidCustomer() {
    String[] invalidIds = Stream.concat(
        CustomerIdExamples.INVALID_CUSTOMER_IDS.stream(),
        CustomerIdExamples.VALID_ANONYMOUS_IDS.stream()).
        toArray(String[]::new);
    for (String invalidCustomerId : invalidIds) {
      assertFalse(invalidCustomerId + " is a valid customer identifier",
          BaseSupportConfig.isConfluentCustomer(invalidCustomerId));
    }
  }

  @Test
  public void testValidAnonymousUser() {
    for (String validId : CustomerIdExamples.VALID_ANONYMOUS_IDS) {
      assertTrue(validId + " is an invalid anonymous user identifier",
          BaseSupportConfig.isAnonymousUser(validId));
    }
  }

  @Test
  public void testInvalidAnonymousUser() {
    String[] invalidIds = Stream.concat(
        CustomerIdExamples.INVALID_ANONYMOUS_IDS.stream(),
        CustomerIdExamples.VALID_CUSTOMER_IDS.stream()).
        toArray(String[]::new);
    for (String invalidId : invalidIds) {
      assertFalse(invalidId + " is a valid anonymous user identifier",
          BaseSupportConfig.isAnonymousUser(invalidId));
    }
  }

  @Test
  public void testCustomerIdValidSettings() {
    String[] validValues = Stream.concat(
        CustomerIdExamples.VALID_ANONYMOUS_IDS.stream(),
        CustomerIdExamples.VALID_CUSTOMER_IDS.stream()).
        toArray(String[]::new);
    for (String validValue : validValues) {
      assertTrue(validValue + " is an invalid value for " +
              BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
          BaseSupportConfig.isSyntacticallyCorrectCustomerId(validValue));
      // old customer Ids are all case-insensitive
      assertFalse(validValue + " is case-sensitive customer ID.",
          BaseSupportConfig.isCaseSensitiveCustomerId(validValue));
    }
  }

  @Test
  public void testCustomerIdInvalidSettings() {
    String[] invalidValues = Stream.concat(
        CustomerIdExamples.INVALID_ANONYMOUS_IDS.stream(),
        CustomerIdExamples.INVALID_CUSTOMER_IDS.stream()).
        toArray(String[]::new);
    for (String invalidValue : invalidValues) {
      assertFalse(invalidValue + " is a valid value for " +
              BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
          BaseSupportConfig.isSyntacticallyCorrectCustomerId(invalidValue));
    }
  }

  @Test
  public void testCaseInsensitiveNewCustomerIds() {
    for (String validValue : CustomerIdExamples.VALID_CASE_INSENSISTIVE_NEW_CUSTOMER_IDS) {
      assertFalse(validValue + " is case-sensitive customer ID.",
          BaseSupportConfig.isCaseSensitiveCustomerId(validValue));
    }
  }

  @Test
  public void testCaseSensitiveNewCustomerIds() {
    for (String validValue : CustomerIdExamples.VALID_CASE_SENSISTIVE_NEW_CUSTOMER_IDS) {
      assertTrue(validValue + " is case-insensitive customer ID.",
          BaseSupportConfig.isCaseSensitiveCustomerId(validValue));
    }
  }

  @Test
  public void testGetDefaultProps() {
    // Given
    Properties overrideProps = new Properties();
    // When
    BaseSupportConfig supportConfig = new TestSupportConfig(overrideProps);

    // Then
    assertTrue(supportConfig.getMetricsEnabled());
    assertEquals("anonymous", supportConfig.getCustomerId());
    assertEquals(24 * 60 * 60 * 1000, supportConfig.getReportIntervalMs());
    assertTrue(supportConfig.isHttpEnabled());
    assertTrue(supportConfig.isHttpsEnabled());
    assertTrue(supportConfig.isProactiveSupportEnabled());
    assertEquals("", supportConfig.getProxy());
    assertEquals("http://support-metrics.confluent.io/anon", supportConfig.getEndpointHttp());
    assertEquals("https://support-metrics.confluent.io/anon", supportConfig.getEndpointHttps());
  }

  @Test
  public void testMergeAndValidatePropsFilterDisallowedKeys() {
    // Given
    Properties overrideProps = new Properties();
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG,
        "anyValue"
    );
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG,
        "anyValue"
    );
    // When
    BaseSupportConfig supportConfig = new TestSupportConfig(overrideProps);

    // Then
    assertEquals("http://support-metrics.confluent.io/anon", supportConfig.getEndpointHttp());
    assertEquals("https://support-metrics.confluent.io/anon", supportConfig.getEndpointHttps());
  }

  @Test
  public void testMergeAndValidatePropsDisableEndpoints() {
    // Given
    Properties overrideProps = new Properties();
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG, "false");
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "false");
    // When
    BaseSupportConfig supportConfig = new TestSupportConfig(overrideProps);

    // Then
    assertTrue(supportConfig.getEndpointHttp().isEmpty());
    assertTrue(supportConfig.getEndpointHttps().isEmpty());
  }

  @Test
  public void testOverrideReportInterval() {
    // Given
    Properties overrideProps = new Properties();
    int reportIntervalHours = 1;
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG,
        String.valueOf(reportIntervalHours)
    );
    // When
    BaseSupportConfig supportConfig = new TestSupportConfig(overrideProps);

    // Then
    assertEquals((long) reportIntervalHours * 60 * 60 * 1000, supportConfig.getReportIntervalMs());
  }

  @Test
  public void isProactiveSupportEnabledFull() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG, "true");

    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertTrue(supportConfig.isProactiveSupportEnabled());
  }

  @Test
  public void isProactiveSupportDisabledFull() {
    // Given
    Properties serverProperties = new Properties();

    serverProperties
        .setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG, "false");
    serverProperties.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG,
        "true"
    );
    serverProperties.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG,
        "true"
    );
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertFalse(supportConfig.isProactiveSupportEnabled());
  }

  @Test
  public void isProactiveSupportEnabledTopicOnly() {
    // Given
    Properties serverProperties = new Properties();
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertTrue(supportConfig.isProactiveSupportEnabled());
  }

  @Test
  public void isProactiveSupportEnabledHTTPOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG,
        "true"
    );
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertTrue(supportConfig.isProactiveSupportEnabled());
  }

  @Test
  public void isProactiveSupportEnabledHTTPSOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "true");
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertTrue(supportConfig.isProactiveSupportEnabled());
  }

  @Test
  public void proactiveSupportIsDisabledByDefaultWhenBrokerConfigurationIsEmpty() {
    // Given
    Properties serverProperties = new Properties();
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertTrue(supportConfig.isProactiveSupportEnabled());
  }

  public static class TestSupportConfig extends BaseSupportConfig {

    public TestSupportConfig(Properties originals) {
      super(originals);
    }

    @Override
    protected String getAnonymousEndpoint(boolean secure) {
      if (!secure) {
        return "http://support-metrics.confluent.io/anon";
      } else {
        return "https://support-metrics.confluent.io/anon";
      }
    }

    @Override
    protected String getTestEndpoint(boolean secure) {
      if (!secure) {
        return "http://support-metrics.confluent.io/test";
      } else {
        return "https://support-metrics.confluent.io/test";
      }
    }

    @Override
    protected String getCustomerEndpoint(boolean secure) {
      if (!secure) {
        return "http://support-metrics.confluent.io/submit";
      } else {
        return "https://support-metrics.confluent.io/submit";
      }
    }
  }

}
