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

package io.confluent.ksql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.Ksql.CliBuilder;
import io.confluent.ksql.Ksql.KsqlClientBuilder;
import io.confluent.ksql.cli.Cli;
import io.confluent.ksql.cli.Options;
import io.confluent.ksql.cli.console.OutputFormat;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.test.util.KsqlTestFolder;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Properties;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlTest {

  @ClassRule
  public static final TemporaryFolder TMP = KsqlTestFolder.temporaryFolder();

  @Mock
  private Options options;
  @Mock
  private KsqlClientBuilder clientBuilder;
  @Mock
  private KsqlRestClient client;
  @Mock
  private CliBuilder cliBuilder;
  @Mock
  private Cli cli;
  private Properties systemProps;
  private Ksql ksql;

  @Before
  public void setUp() {
    systemProps = new Properties();

    ksql = new Ksql(options, systemProps, clientBuilder, cliBuilder);

    when(options.getOutputFormat()).thenReturn(OutputFormat.TABULAR);
    when(clientBuilder.build(any(), any(), any(), any(), any())).thenReturn(client);
    when(cliBuilder.build(any(), any(), any(), any())).thenReturn(cli);
  }

  @Test
  public void shouldRunInteractively() {
    // When:
    ksql.run();

    // Then:
    verify(cli).runInteractively();
  }

  @Test
  public void shouldAddDefinedVariablesToCliBeforeRunningCommands() {
    // Given:
    when(options.getVariables()).thenReturn(ImmutableMap.of("env", "qa"));
    when(options.getExecute()).thenReturn(Optional.of("this is a command"));

    // When:
    ksql.run();

    // Then:
    final InOrder inOrder = Mockito.inOrder(cli);
    inOrder.verify(cli).addSessionVariables(ImmutableMap.of("env", "qa"));
    inOrder.verify(cli).runCommand("this is a command");
  }

  @Test
  public void shouldRunNonInteractiveCommandWhenExecuteOptionIsUsed() {
    // Given:
    when(options.getExecute()).thenReturn(Optional.of("this is a command"));

    // When:
    ksql.run();

    // Then:
    verify(cli).runCommand("this is a command");
  }

  @Test
  public void shouldRunScriptFileWhenFileOptionIsUsed() throws IOException {
    // Given:
    final String sqlFile = TMP.newFile().getAbsolutePath();
    when(options.getScriptFile()).thenReturn(Optional.of(sqlFile));

    // When:
    ksql.run();

    // Then:
    verify(cli).runScript(sqlFile);
  }

  @Test
  public void shouldBuildClientWithCorrectServerAddress() {
    // Given:
    when(options.getServer()).thenReturn("in a galaxy far far away");

    // When:
    ksql.run();

    // Then:
    verify(clientBuilder).build(eq("in a galaxy far far away"), any(), any(), any(), any());
  }

  @Test
  public void shouldSupportSslConfigInConfigFile() throws Exception {
    // Given:
    givenConfigFile(
        "ssl.truststore.location=some/path" + System.lineSeparator()
            + "ssl.truststore.password=letmein"
    );

    // When:
    ksql.run();

    // Then:
    verify(clientBuilder).build(any(), any(), eq(ImmutableMap.of(
        "ssl.truststore.location", "some/path",
        "ssl.truststore.password", "letmein"
    )), any(), any());
  }

  @Test
  public void shouldUseSslConfigInSystemConfigInPreferenceToAnyInConfigFile() throws Exception {
    // Given:
    givenConfigFile(
        "ssl.truststore.location=should not use" + System.lineSeparator()
            + "ssl.truststore.password=should not use"
    );

    givenSystemProperties(
        "ssl.truststore.location", "some/path",
        "ssl.truststore.password", "letmein"
    );

    // When:
    ksql.run();

    // Then:
    verify(clientBuilder).build(any(), any(), eq(ImmutableMap.of(
        "ssl.truststore.location", "some/path",
        "ssl.truststore.password", "letmein"
    )), any(), any());
  }

  @Test
  public void shouldStripSslConfigFromConfigFileWhenMakingLocalProperties() throws Exception {
    // Given:
    givenConfigFile(
        "ssl.truststore.location=some/path" + System.lineSeparator()
            + "ssl.truststore.password=letmein" + System.lineSeparator()
            + "some.other.setting=value"
    );

    // When:
    ksql.run();

    // Then:
    verify(clientBuilder).build(any(), eq(ImmutableMap.of("some.other.setting", "value")), any(), any(), any());
  }

  private void givenConfigFile(final String content) throws Exception {
    final File file = TMP.newFile();
    when(options.getConfigFile()).thenReturn(Optional.of(file.getAbsolutePath()));

    Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
  }

  private void givenSystemProperties(final String... s) {
    assertThat(s.length % 2, is(0));

    for (int i = 0; i < s.length; i = i + 2) {
      systemProps.setProperty(s[i], s[i + 1]);
    }
  }
}