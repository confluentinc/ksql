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

package io.confluent.ksql.rest.server;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.easymock.Capture;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class KsqlServerMainTest {
  private KsqlServerMain main;

  @Mock(MockType.NICE)
  private Executable executable;
  @Mock(MockType.NICE)
  private Executable precondition;
  @Mock(MockType.NICE)
  private Executor shutdownHandler;

  private final File mockStreamsStateDir = mock(File.class);

  @Before
  public void setUp() {
    main = new KsqlServerMain(precondition, () -> executable, shutdownHandler);
    when(mockStreamsStateDir.exists()).thenReturn(true);
    when(mockStreamsStateDir.mkdirs()).thenReturn(true);
    when(mockStreamsStateDir.isDirectory()).thenReturn(true);
    when(mockStreamsStateDir.canWrite()).thenReturn(true);
    when(mockStreamsStateDir.canExecute()).thenReturn(true);
    when(mockStreamsStateDir.getPath()).thenReturn("/var/lib/kafka-streams");
  }

  @Test
  public void shouldStopAppOnJoin() throws Exception {
    // Given:
    executable.shutdown();
    expectLastCall();
    replay(executable);

    // When:
    main.tryStartApp();

    // Then:
    verify(executable);
  }

  @Test
  public void shouldStopAppOnErrorStarting() throws Exception {
    // Given:
    executable.startAsync();
    expectLastCall().andThrow(new RuntimeException("Boom"));

    executable.shutdown();
    expectLastCall();
    replay(executable);

    try {
      // When:
      main.tryStartApp();
      fail();
    } catch (final Exception e) {
      // Expected
    }

    // Then:
    verify(executable);
  }

  @Test
  public void shouldNotifyAppOnTerminate() throws Exception {
    // Given:
    final Capture<Runnable> captureShutdownHandler = newCapture();
    shutdownHandler.execute(capture(captureShutdownHandler));
    expectLastCall();
    precondition.notifyTerminated();
    expectLastCall();
    replay(shutdownHandler, precondition);
    main.tryStartApp();
    final Runnable handler = captureShutdownHandler.getValue();

    // When:
    handler.run();

    // Then:
    verify(precondition);
  }

  @Test
  public void shouldFailIfStreamsStateDirectoryCannotBeCreated() {
    // Given:
    when(mockStreamsStateDir.exists()).thenReturn(false);
    when(mockStreamsStateDir.mkdirs()).thenReturn(false);

    // When:
    final Exception e = assertThrows(
        KsqlServerException.class,
        () -> KsqlServerMain.enforceStreamStateDirAvailability(mockStreamsStateDir)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Could not create the kafka streams state directory: /var/lib/kafka-streams\n"
        + " Make sure the directory exists and is writable for KSQL server \n"
        + " or its parent directory is writable by KSQL server\n"
        + " or change it to a writable directory by setting 'ksql.streams.state.dir' config in"
        + " the properties file."));
  }

  @Test
  public void shouldFailIfStreamsStateDirectoryIsNotDirectory() {
    // Given:
    when(mockStreamsStateDir.isDirectory()).thenReturn(false);

    // When:
    final Exception e = assertThrows(
        KsqlServerException.class,
        () -> KsqlServerMain.enforceStreamStateDirAvailability(mockStreamsStateDir)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "/var/lib/kafka-streams is not a directory.\n"
            + " Make sure the directory exists and is writable for KSQL server \n"
            + " or its parent directory is writable by KSQL server\n"
            + " or change it to a writable directory by setting 'ksql.streams.state.dir' config in"
            + " the properties file."));
  }

  @Test
  public void shouldFailIfStreamsStateDirectoryIsNotWritable() {
    // Given:
    when(mockStreamsStateDir.canWrite()).thenReturn(false);

    // When:
    final Exception e = assertThrows(
        KsqlServerException.class,
        () -> KsqlServerMain.enforceStreamStateDirAvailability(mockStreamsStateDir)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "The kafka streams state directory is not writable for KSQL server: /var/lib/kafka-streams\n"
            + " Make sure the directory exists and is writable for KSQL server \n"
            + " or change it to a writable directory by setting 'ksql.streams.state.dir' "
            + "config in the properties file."));
  }

  @Test
  public void shouldFailIfStreamsStateDirectoryIsNotExacutable() {
    // Given:
    when(mockStreamsStateDir.canExecute()).thenReturn(false);

    // When:
    final Exception e = assertThrows(
        KsqlServerException.class,
        () -> KsqlServerMain.enforceStreamStateDirAvailability(mockStreamsStateDir)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "The kafka streams state directory is not writable for KSQL server: /var/lib/kafka-streams\n"
            + " Make sure the directory exists and is writable for KSQL server \n"
            + " or change it to a writable directory by setting 'ksql.streams.state.dir' "
            + "config in the properties file."));
  }

  @Test
  public void shouldValidateDefaultFormatsWithCaseInsensitivity() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG, "avro"
    ));

    // When:
    KsqlServerMain.validateDefaultTopicFormats(config);

    // Then: No exception
  }

  @Test
  public void shouldValidateEmptyDefaultFormat() {
    // Given:
    // Default value format is empty
    final KsqlConfig config = configWith(ImmutableMap.of());

    // When:
    KsqlServerMain.validateDefaultTopicFormats(config);

    // Then: No exception
  }

  @Test
  public void shouldFailOnInvalidDefaultKeyFormat() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG, "bad"
    ));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> KsqlServerMain.validateDefaultTopicFormats(config)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Invalid value for config '" + KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG + "': bad"));
  }

  @Test
  public void shouldFailOnInvalidDefaultValueFormat() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG, "bad"
    ));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> KsqlServerMain.validateDefaultTopicFormats(config)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Invalid value for config '" + KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG + "': bad"));
  }

  @Test
  public void shouldFailOnInvalidCipherSuitesList() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        ConfluentConfigs.ENABLE_FIPS_CONFIG, true
    ));
    final String wrongCipherSuite = "TLS_RSA_WITH_NULL_MD5";
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.SSL_CIPHER_SUITES_CONFIG,
            Collections.singletonList(wrongCipherSuite))
        .build()
    );

    // When:
    final Exception e = assertThrows(
        SecurityException.class,
        () -> KsqlServerMain.validateFips(config, restConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "FIPS 140-2 Configuration Error, invalid cipher suites: "
            + wrongCipherSuite));
  }

  @Test
  public void shouldFailOnInvalidEnabledProtocols() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        ConfluentConfigs.ENABLE_FIPS_CONFIG, true
    ));
    final String wrongEnabledProtocols = "TLSv1.0";
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.SSL_CIPHER_SUITES_CONFIG,
            Collections.singletonList("TLS_RSA_WITH_AES_256_CCM"))
        .put(KsqlRestConfig.SSL_ENABLED_PROTOCOLS_CONFIG,
            Collections.singletonList(wrongEnabledProtocols))
        .build()
    );

    // When:
    final Exception e = assertThrows(
        SecurityException.class,
        () -> KsqlServerMain.validateFips(config, restConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "FIPS 140-2 Configuration Error, invalid TLS versions: "
            + wrongEnabledProtocols));
  }

  @Test
  public void shouldFailOnNullBrokerSecurityProtocol() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        ConfluentConfigs.ENABLE_FIPS_CONFIG, true
    ));
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.SSL_CIPHER_SUITES_CONFIG,
            Collections.singletonList("TLS_RSA_WITH_AES_256_CCM"))
        .build()
    );

    // When:
    final Exception e = assertThrows(
        SecurityException.class,
        () -> KsqlServerMain.validateFips(config, restConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "The security protocol ('"
            + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
            + "') is not specified."));
  }

  @Test
  public void shouldFailOnInvalidBrokerSecurityProtocol() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        ConfluentConfigs.ENABLE_FIPS_CONFIG, true,
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name
    ));
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.SSL_CIPHER_SUITES_CONFIG,
            Collections.singletonList("TLS_RSA_WITH_AES_256_CCM"))
        .build()
    );

    // When:
    final Exception e = assertThrows(
        SecurityException.class,
        () -> KsqlServerMain.validateFips(config, restConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "FIPS 140-2 Configuration Error, invalid broker protocols: "
            + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
            + ":SASL_PLAINTEXT"));
  }

  @Test
  public void shouldFailOnNullSSLEndpointIdentificationAlgorithm() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        ConfluentConfigs.ENABLE_FIPS_CONFIG, true,
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name
    ));
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.SSL_CIPHER_SUITES_CONFIG,
            Collections.singletonList("TLS_RSA_WITH_AES_256_CCM"))
        .build()
    );

    // When:
    final Exception e = assertThrows(
        SecurityException.class,
        () -> KsqlServerMain.validateFips(config, restConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "The SSL endpoint identification algorithm ('"
            + KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG
            + "') is not specified."));
  }

  @Test
  public void shouldFailOnInvalidSSLEndpointIdentificationAlgorithm() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        ConfluentConfigs.ENABLE_FIPS_CONFIG, true,
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name
    ));
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.SSL_CIPHER_SUITES_CONFIG,
            Collections.singletonList("TLS_RSA_WITH_AES_256_CCM"))
        .put(KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "http")
        .build()
    );

    // When:
    final Exception e = assertThrows(
        SecurityException.class,
        () -> KsqlServerMain.validateFips(config, restConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "FIPS 140-2 Configuration Error, invalid rest protocol: http"
        + "\nInvalid rest protocol for "
        + KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG));
  }

  @Test
  public void shouldFailOnNullSRUrl() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        ConfluentConfigs.ENABLE_FIPS_CONFIG, true,
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name
    ));
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.SSL_CIPHER_SUITES_CONFIG,
            Collections.singletonList("TLS_RSA_WITH_AES_256_CCM"))
        .put(KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https")
        .build()
    );

    // When:
    final Exception e = assertThrows(
        SecurityException.class,
        () -> KsqlServerMain.validateFips(config, restConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "The Ksql schema registry url property "
            + "('"
            + KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY
            + "') is not specified."));
  }

  @Test
  public void shouldFailOnInvalidSRUrl() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        ConfluentConfigs.ENABLE_FIPS_CONFIG, true,
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name
    ));
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.SSL_CIPHER_SUITES_CONFIG,
            Collections.singletonList("TLS_RSA_WITH_AES_256_CCM"))
        .put(KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https")
        .put(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "http://foo:8080")
        .build()
    );


    // When:
    final Exception e = assertThrows(
        SecurityException.class,
        () -> KsqlServerMain.validateFips(config, restConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "FIPS 140-2 Configuration Error, invalid rest protocol: http"
            + "\nInvalid rest protocol for "
            + KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY));
  }

  @Test
  public void shouldFailOnInvalidListenerProtocols() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        ConfluentConfigs.ENABLE_FIPS_CONFIG, true,
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name
    ));
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.SSL_CIPHER_SUITES_CONFIG,
            Collections.singletonList("TLS_RSA_WITH_AES_256_CCM"))
        .put(KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https")
        .put(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "https://foo:8080")
        .build()
    );


    // When:
    final Exception e = assertThrows(
        SecurityException.class,
        () -> KsqlServerMain.validateFips(config, restConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "FIPS 140-2 Configuration Error, invalid rest protocol: http"
            + "\nInvalid rest protocol for listeners."
            + "\nMake sure that all listeners, listeners.proxy.protocol, ksql.advertised.listener, and ksql.internal.listener follow FIPS 140-2."));
  }

  @Test
  public void shouldFailOnInvalidProxyListenerProtocols() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        ConfluentConfigs.ENABLE_FIPS_CONFIG, true,
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name
    ));
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.SSL_CIPHER_SUITES_CONFIG,
            Collections.singletonList("TLS_RSA_WITH_AES_256_CCM"))
        .put(KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https")
        .put(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "https://foo:8080")
        .put(KsqlRestConfig.LISTENERS_CONFIG,
            Collections.singletonList("https://bar:8080"))
        .put(KsqlRestConfig.PROXY_PROTOCOL_LISTENERS_CONFIG,
            Collections.singletonList("http://baz:8080"))
        .build()
    );


    // When:
    final Exception e = assertThrows(
        SecurityException.class,
        () -> KsqlServerMain.validateFips(config, restConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "FIPS 140-2 Configuration Error, invalid rest protocol: http"
            + "\nInvalid rest protocol for listeners."
            + "\nMake sure that all listeners, listeners.proxy.protocol, ksql.advertised.listener, and ksql.internal.listener follow FIPS 140-2."));
  }

  @Test
  public void shouldFailOnInvalidInternalListenerProtocols() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        ConfluentConfigs.ENABLE_FIPS_CONFIG, true,
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name
    ));
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.SSL_CIPHER_SUITES_CONFIG,
            Collections.singletonList("TLS_RSA_WITH_AES_256_CCM"))
        .put(KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https")
        .put(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "https://foo:8080")
        .put(KsqlRestConfig.LISTENERS_CONFIG,
            Collections.singletonList("https://bar:8080"))
        .put(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://baz:8080")
        .build()
    );


    // When:
    final Exception e = assertThrows(
        SecurityException.class,
        () -> KsqlServerMain.validateFips(config, restConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "FIPS 140-2 Configuration Error, invalid rest protocol: http"
            + "\nInvalid rest protocol for listeners."
            + "\nMake sure that all listeners, listeners.proxy.protocol, ksql.advertised.listener, and ksql.internal.listener follow FIPS 140-2."));
  }

  @Test
  public void shouldFailOnInvalidAdvertisedListenerProtocols() {
    // Given:
    final KsqlConfig config = configWith(ImmutableMap.of(
        ConfluentConfigs.ENABLE_FIPS_CONFIG, true,
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name
    ));
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.SSL_CIPHER_SUITES_CONFIG,
            Collections.singletonList("TLS_RSA_WITH_AES_256_CCM"))
        .put(KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https")
        .put(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "https://foo:8080")
        .put(KsqlRestConfig.LISTENERS_CONFIG,
            Collections.singletonList("https://bar:8080"))
        .put(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://baz:8080")
        .build()
    );


    // When:
    final Exception e = assertThrows(
        SecurityException.class,
        () -> KsqlServerMain.validateFips(config, restConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "FIPS 140-2 Configuration Error, invalid rest protocol: http"
            + "\nInvalid rest protocol for listeners."
            + "\nMake sure that all listeners, listeners.proxy.protocol, ksql.advertised.listener, and ksql.internal.listener follow FIPS 140-2."));
  }

  private static KsqlConfig configWith(final Map<String, Object> additionalConfigs) {
    return KsqlConfigTestUtil.create("unused", additionalConfigs);
  }

}
