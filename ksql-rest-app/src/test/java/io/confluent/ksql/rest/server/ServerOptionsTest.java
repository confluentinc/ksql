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

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class ServerOptionsTest {

  private ServerOptions serverOptions;

  @Before
  public void setUp() {
    serverOptions = new ServerOptions();
  }

  @Test
  public void shouldNotHaveQueriesFileIfNotInPropertiesOrCommandLine() {
    assertThat(serverOptions.getQueriesFile(emptyMap()), is(Optional.empty()));
  }

  @Test
  public void shouldHaveQueriesFileIfInProperties() {
    // Given:
    final Map<String, String> propsFile = ImmutableMap.of(
        ServerOptions.QUERIES_FILE_CONFIG, "/path/to/file"
    );

    // Then:
    assertThat(serverOptions.getQueriesFile(propsFile), is(Optional.of("/path/to/file")));
  }

  @Test
  public void shouldHaveQueriesFileIfSpecifiedOnCmdLine() throws IOException {
    // Given:
    final String queryFilePath = "/path/to/query-file";

    // When:
    serverOptions = ServerOptions.parse("config.file", "--queries-file", queryFilePath);

    // Then:
    assertThat(serverOptions.getQueriesFile(emptyMap()), is(Optional.of(queryFilePath)));
  }

  @Test
  public void shouldUseQueryFileParamFromCmdLineInPreferenceToProperties() throws IOException {
    // Given:
    final String cmdLineArg = "/path/to/query-file";

    final Map<String, String> propsFile = ImmutableMap.of(
        ServerOptions.QUERIES_FILE_CONFIG, "should not use this"
    );

    // When:
    serverOptions = ServerOptions.parse("config.file", "--queries-file", cmdLineArg);

    // Then:
    assertThat(serverOptions.getQueriesFile(propsFile), is(Optional.of(cmdLineArg)));
  }
}