/**
 * Copyright 2017 Confluent Inc.
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
package io.confluent.ksql.cli.commands;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;

import org.apache.kafka.streams.StreamsConfig;

import io.confluent.ksql.cli.Cli;
import io.confluent.ksql.cli.RemoteCli;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.cli.console.JLineTerminal;
import io.confluent.ksql.support.metrics.KsqlSupportMetricsAgent;
import io.confluent.ksql.support.metrics.collector.KsqlModuleType;
import io.confluent.ksql.util.KsqlConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Command(name = "remote", description = "Connect to a remote (possibly distributed) Ksql session")
public class Remote extends AbstractCliCommands {

  @Once
  @Required
  @Arguments(
      title = "server",
      description = "The address of the Ksql server to connect to (ex: http://confluent.io:9098)"
  )
  String server;
  private static final String PROPERTIES_FILE_OPTION_NAME = "--properties-file";

  @Option(
      name = PROPERTIES_FILE_OPTION_NAME,
      description = "A file specifying properties for Ksql and its underlying Kafka Streams "
                    + "instance(s) (can specify port number, bootstrap server, etc. "
                    + "but these options will "
                    + "be overridden if also given via  flags)"
  )
  String propertiesFile;

  @Override
  public Cli getCli() throws Exception {
    Map<String, Object> propertiesMap = new HashMap<>();
    Properties properties = getStandaloneProperties();
    for (String key: properties.stringPropertyNames()) {
      propertiesMap.put(key, properties.getProperty(key));
    }

    KsqlRestClient restClient = new KsqlRestClient(server, propertiesMap);
    Console terminal = new JLineTerminal(parseOutputFormat(), restClient);

    KsqlSupportMetricsAgent.initialize(KsqlModuleType.REMOTE_CLI, properties);
    return new RemoteCli(
        streamedQueryRowLimit,
        streamedQueryTimeoutMs,
        restClient,
        terminal
    );
  }

  private Properties getStandaloneProperties() throws IOException {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, KsqlConfig.KSQL_SERVICE_ID_DEFAULT);
    addFileProperties(properties);
    return properties;
  }

  private void addFileProperties(Properties properties) throws IOException {
    if (propertiesFile != null) {
      try(final FileInputStream input = new FileInputStream(propertiesFile)) {
        properties.load(input);
      }
      if (properties.containsKey(KsqlConfig.KSQL_SERVICE_ID_CONFIG)) {
        properties
            .put(StreamsConfig.APPLICATION_ID_CONFIG,
                 properties.getProperty(KsqlConfig.KSQL_SERVICE_ID_CONFIG));
      } else {
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, KsqlConfig.KSQL_SERVICE_ID_DEFAULT);
      }
    }
  }
}
