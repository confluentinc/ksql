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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Configuration for the Confluent Support options.
 */
public abstract class BaseSupportConfig {

  private static final Logger log = LogManager
      .getLogger(io.confluent.support.metrics.BaseSupportConfig.class);

  static final String CONFLUENT_PHONE_HOME_ENDPOINT_BASE_SECURE =
      "https://version-check.confluent.io";
  static final String CONFLUENT_PHONE_HOME_ENDPOINT_BASE_INSECURE =
      "http://version-check.confluent.io";
  public static final String CONFLUENT_PHONE_HOME_ENDPOINT_SUFFIX_USER_ANON = "anon";
  public static final String CONFLUENT_PHONE_HOME_ENDPOINT_SUFFIX_USER_TEST = "test";
  public static final String CONFLUENT_PHONE_HOME_ENDPOINT_SUFFIX_USER_CUSTOMER = "submit";

  /**
   * <code>confluent.support.metrics.enable</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG =
      "confluent.support.metrics.enable";
  private static final String CONFLUENT_SUPPORT_METRICS_ENABLE_DOC =
      "False to disable metric collection, true otherwise.";
  public static final String CONFLUENT_SUPPORT_METRICS_ENABLE_DEFAULT = "true";

  /**
   * <code>confluent.support.customer.id</code>
   */
  public static final String CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG = "confluent.support.customer.id";
  private static final String CONFLUENT_SUPPORT_CUSTOMER_ID_DOC =
      "Customer ID assigned by Confluent";
  public static final String CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT = "anonymous";
  public static final String CONFLUENT_SUPPORT_TEST_ID_DEFAULT = "c0";

  /**
   * <code>confluent.support.metrics.report.interval.hours</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG =
      "confluent.support.metrics.report.interval.hours";
  private static final String CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_DOC =
      "Frequency of reporting in hours, e.g., 24 would indicate every day ";
  public static final String CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_DEFAULT = "24";

  /**
   * <code>confluent.support.metrics.endpoint.insecure.enable</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG =
      "confluent.support.metrics.endpoint.insecure.enable";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_DOC =
      "False to disable reporting over HTTP, true otherwise";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_DEFAULT = "true";

  /**
   * <code>confluent.support.metrics.endpoint.secure.enable</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG =
      "confluent.support.metrics.endpoint.secure.enable";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_DOC =
      "False to disable reporting over HTTPS, true otherwise";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_DEFAULT = "true";

  /**
   * <code>confluent.support.proxy</code>
   */
  public static final String CONFLUENT_SUPPORT_PROXY_CONFIG = "confluent.support.proxy";
  public static final String CONFLUENT_SUPPORT_PROXY_DOC =
      "HTTP forward proxy used to support metrics to Confluent";
  public static final String CONFLUENT_SUPPORT_PROXY_DEFAULT = "";


  /**
   * Confluent endpoints. These are internal properties that cannot be set from a config file but
   * that are added to the original config file at startup time
   */
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG =
      "confluent.support.metrics.endpoint.insecure";

  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG =
      "confluent.support.metrics.endpoint.secure";


  private static final Pattern CUSTOMER_PATTERN = Pattern.compile("c\\d{1,30}");
  private static final Pattern NEW_CUSTOMER_CASE_SENSISTIVE_PATTERN = Pattern.compile(
      "[a-zA-Z0-9]{15}"
  );
  private static final Pattern NEW_CUSTOMER_CASE_INSENSISTIVE_PATTERN = Pattern.compile(
      "[a-zA-Z0-9]{18}"
  );

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public Properties getProperties() {
    return properties;
  }

  private Properties properties;


  /**
   * Returns the default Proactive Support properties
   */
  protected Properties getDefaultProps() {
    final Properties props = new Properties();
    props.setProperty(
        CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG,
        CONFLUENT_SUPPORT_METRICS_ENABLE_DEFAULT
    );
    props.setProperty(CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG, CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
    props.setProperty(
        CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG,
        CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_DEFAULT
    );
    props.setProperty(
        CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG,
        CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_DEFAULT
    );
    props.setProperty(
        CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG,
        CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_DEFAULT
    );
    props.setProperty(CONFLUENT_SUPPORT_PROXY_CONFIG, CONFLUENT_SUPPORT_PROXY_DEFAULT);
    return props;
  }

  public BaseSupportConfig(final Properties originals) {
    mergeAndValidateWithDefaultProperties(originals, null);
  }

  public BaseSupportConfig(final Properties originals, final String endpointPath) {
    mergeAndValidateWithDefaultProperties(originals, endpointPath);
  }

  /**
   * Takes default properties from getDefaultProps() and a set of override properties and returns a
   * merged properties object, where the defaults are overridden. Sanitizes and validates the
   * returned properties
   *
   * @param overrides Parameters that override the default properties
   */
  private void mergeAndValidateWithDefaultProperties(final Properties overrides,
      final String endpointPath) {
    final Properties defaults = getDefaultProps();
    final Properties props;

    if (overrides == null) {
      props = defaults;
    } else {
      props = new Properties();
      props.putAll(defaults);
      props.putAll(overrides);
    }
    this.properties = props;

    // make sure users are not setting internal properties in the config file
    // sanitize props just in case
    props.remove(CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG);
    props.remove(CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG);

    final String customerId = getCustomerId();

    // new way to set endpoint
    if (endpointPath != null) {
      // derived class provided URI
      if (isHttpEnabled()) {
        setEndpointHttp(getEndpoint(false, customerId, endpointPath));
      }
      if (isHttpsEnabled()) {
        setEndpointHttps(getEndpoint(true, customerId, endpointPath));
      }
    } else {
      // set the correct customer id/endpoint pair
      setEndpointsOldWay(customerId);
    }
  }

  private void setEndpointsOldWay(final String customerId) {
    if (isAnonymousUser(customerId)) {
      if (isHttpEnabled()) {
        setEndpointHttp(getAnonymousEndpoint(false));
      }
      if (isHttpsEnabled()) {
        setEndpointHttps(getAnonymousEndpoint(true));
      }
    } else {
      if (isTestUser(customerId)) {
        if (isHttpEnabled()) {
          setEndpointHttp(getTestEndpoint(false));
        }
        if (isHttpsEnabled()) {
          setEndpointHttps(getTestEndpoint(true));
        }
      } else {
        if (isHttpEnabled()) {
          setEndpointHttp(getCustomerEndpoint(false));
        }
        if (isHttpsEnabled()) {
          setEndpointHttps(getCustomerEndpoint(true));
        }
      }
    }
  }

  protected String getAnonymousEndpoint(final boolean secure) {
    return null;
  }

  protected String getTestEndpoint(final boolean secure) {
    return null;
  }

  protected String getCustomerEndpoint(final boolean secure) {
    return null;
  }

  public static String getEndpoint(final boolean secure, final String customerId,
      final String endpointPath) {
    final String base = secure ? CONFLUENT_PHONE_HOME_ENDPOINT_BASE_SECURE
        : CONFLUENT_PHONE_HOME_ENDPOINT_BASE_INSECURE;
    return base + "/" + endpointPath + "/" + getEndpointSuffix(customerId);
  }

  private static String getEndpointSuffix(final String customerId) {
    if (isAnonymousUser(customerId)) {
      return CONFLUENT_PHONE_HOME_ENDPOINT_SUFFIX_USER_ANON;
    } else {
      if (isTestUser(customerId)) {
        return CONFLUENT_PHONE_HOME_ENDPOINT_SUFFIX_USER_TEST;
      } else {
        return CONFLUENT_PHONE_HOME_ENDPOINT_SUFFIX_USER_CUSTOMER;
      }
    }
  }

  /**
   * A check on whether Proactive Support (PS) is enabled or not. PS is disabled when
   * CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG is false or is not defined
   *
   * @return false if PS is not enabled, true if PS is enabled
   */
  public boolean isProactiveSupportEnabled() {
    if (properties == null) {
      return false;
    }
    return getMetricsEnabled();
  }

  /**
   * @param customerId The value of "confluent.support.customer.id".
   * @return True if the value matches the setting we use to denote anonymous users.
   */
  public static boolean isAnonymousUser(final String customerId) {
    return customerId != null && customerId.toLowerCase(Locale.ROOT).equals(
        CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
  }

  /**
   * @param customerId The value of "confluent.support.customer.id".
   * @return True if the value matches the setting we use to denote internal testing.
   */
  public static boolean isTestUser(final String customerId) {
    return customerId != null && customerId.toLowerCase(Locale.ROOT)
        .equals(CONFLUENT_SUPPORT_TEST_ID_DEFAULT);
  }

  /**
   * @param customerId The value of "confluent.support.customer.id".
   * @return True if the value matches the pattern of Confluent's internal customer ids.
   */
  public static boolean isConfluentCustomer(final String customerId) {
    return customerId != null
        && (CUSTOMER_PATTERN.matcher(customerId.toLowerCase(Locale.ROOT)).matches()
        || NEW_CUSTOMER_CASE_INSENSISTIVE_PATTERN.matcher(customerId).matches()
        || NEW_CUSTOMER_CASE_SENSISTIVE_PATTERN.matcher(customerId).matches());
  }

  /**
   * @param customerId The value of "confluent.support.customer.id".
   * @return True if the value is syntactically correct.
   */
  public static boolean isSyntacticallyCorrectCustomerId(final String customerId) {
    return isAnonymousUser(customerId) || isConfluentCustomer(customerId);
  }

  /**
   * The 15-character alpha-numeric customer IDs are case-sensitive, others are case-insensitive.
   * The old-style customer ID "c\\d{1,30}" maybe look the same as a 15-character alpha-numeric ID,
   * if its length is also 15 characters. In that case, this method will return true.
   *
   * @param customerId The value of "confluent.support.customer.id".
   * @return true if customer Id is case sensitive
   */
  public static boolean isCaseSensitiveCustomerId(final String customerId) {
    return NEW_CUSTOMER_CASE_SENSISTIVE_PATTERN.matcher(customerId).matches();
  }

  public String getCustomerId() {
    return getCustomerId(properties);
  }

  public static String getCustomerId(final Properties props) {
    final String fallbackId = CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT;
    String id = props.getProperty(CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG);
    if (id == null || id.isEmpty()) {
      log.info("No customer ID configured -- falling back to id '{}'", fallbackId);
      id = fallbackId;
    }
    if (!isSyntacticallyCorrectCustomerId(id)) {
      log.error(
          "'{}' is not a valid Confluent customer ID -- falling back to id '{}'",
          id,
          fallbackId
      );
      id = fallbackId;
    }
    return id;
  }

  public long getReportIntervalMs() {
    String intervalString =
        properties.getProperty(
            CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG
        );
    if (intervalString == null || intervalString.isEmpty()) {
      intervalString = CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_DEFAULT;
    }
    try {
      final long intervalHours = Long.parseLong(intervalString);
      if (intervalHours < 1) {
        throw new ConfigException(
            CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG,
            intervalString,
            "Interval must be >= 1"
        );
      }
      return intervalHours * 60 * 60 * 1000;
    } catch (NumberFormatException e) {
      throw new ConfigException(
          CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG,
          intervalString,
          "Interval is not an integer number"
      );
    }
  }

  public boolean getMetricsEnabled() {
    final String enableString = properties
        .getProperty(CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG, "false");
    return Boolean.parseBoolean(enableString);
  }

  public boolean isHttpEnabled() {
    final String enableHttp =
        properties.getProperty(
            CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG,
            CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_DEFAULT
        );
    return Boolean.parseBoolean(enableHttp);
  }

  public String getEndpointHttp() {
    return properties
        .getProperty(CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, "");
  }

  public void setEndpointHttp(final String endpointHttp) {
    properties.setProperty(
        CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG,
        endpointHttp
    );
  }

  public boolean isHttpsEnabled() {
    final String enableHttps =
        properties.getProperty(
            CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG,
            CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_DEFAULT
        );
    return Boolean.parseBoolean(enableHttps);
  }

  public String getEndpointHttps() {
    return properties.getProperty(
        CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG,
        ""
    );
  }

  public void setEndpointHttps(final String endpointHttps) {
    properties.setProperty(
        CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG,
        endpointHttps
    );
  }

  public String getProxy() {
    return properties.getProperty(
        CONFLUENT_SUPPORT_PROXY_CONFIG,
        CONFLUENT_SUPPORT_PROXY_DEFAULT
    );
  }
}
