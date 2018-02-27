/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.ksql.rest.server;

import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;

public class ServerOptionsTest {

  @Test
  public void shouldNotHaveQueriesFileIfNotInPropertiesOrCommandLine() {
    final ServerOptions serverOptions = new ServerOptions();
    assertFalse(serverOptions.getQueriesFile(new Properties()).isPresent());
  }

  @Test
  public void shouldHaveQueriesFileIfInProperties() {
    final  Properties properties = new Properties();
    final String queryFilePath = "/path/to/file";
    properties.put(ServerOptions.QUERIES_FILE_CONFIG, queryFilePath);
    final ServerOptions serverOptions = new ServerOptions();
    assertThat(serverOptions.getQueriesFile(properties).get(), equalTo(queryFilePath));
  }

  @Test
  public void shouldHaveQueriesFileIfSpecifiedOnCmdLine() throws IOException {
    final String queryFilePath = "/path/to/query-file";
    final ServerOptions
        options = ServerOptions.parse("config.file", "--queries-file", queryFilePath);
    assertThat(options.getQueriesFile(new Properties()).get(), equalTo(queryFilePath));
  }

  @Test
  public void shouldUseQueryFileParamFromCmdLineInPreferenceToProperties() throws IOException {
    final String cmdLineArg = "/path/to/query-file";
    final ServerOptions
        options = ServerOptions.parse("config.file", "--queries-file", cmdLineArg);
    final Properties properties = new Properties();
    properties.put(ServerOptions.QUERIES_FILE_CONFIG, "blah");
    assertThat(options.getQueriesFile(properties).get(), equalTo(cmdLineArg));

  }
}