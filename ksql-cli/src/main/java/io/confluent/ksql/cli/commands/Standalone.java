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
import io.confluent.ksql.cli.Cli;
import io.confluent.ksql.util.CliUtils;
import io.confluent.ksql.cli.StandaloneExecutor;
import io.confluent.ksql.util.KsqlConfig;

import org.apache.kafka.streams.StreamsConfig;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

@Command(name = "standalone", description = "Running KSQL statements from a file.")
public class Standalone extends AbstractCliCommands {

  private static final String PROPERTIES_FILE_OPTION_NAME = "--properties-file";
  private static final String KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT = "localhost:9092";

  @Option(
      name = PROPERTIES_FILE_OPTION_NAME,
      description = "A file specifying properties for Ksql and its underlying Kafka Streams "
                    + "instance(s) (can specify port number, bootstrap server, etc. "
                    + "but these options will "
                    + "be overridden if also given via  flags)"
  )
  String propertiesFile;

  @Once
  @Required
  @Arguments(
      title = "query-file",
      description = "Path to the query file in the local machine.)"
  )
  String queryFile;

  @Override
  protected Cli getCli() throws Exception {
    return null;
  }

  @Override
  public void run() {
    try {
      CliUtils cliUtils = new CliUtils();
      String queries = cliUtils.readQueryFile(queryFile);
      StandaloneExecutor standaloneExecutor = new StandaloneExecutor(getStandaloneProperties());
      standaloneExecutor.executeStatements(queries);

    } catch (Exception e) {
      if (e.getCause() instanceof FileNotFoundException) {
        System.err.println("Query file " + queryFile + " does not exist");
      } else {
        e.printStackTrace();
      }
    }
  }

  private Properties getStandaloneProperties() throws IOException {
    Properties properties = new Properties();
    addDefaultProperties(properties);
    addFileProperties(properties);
    return properties;
  }

  private void addDefaultProperties(Properties properties) {
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT);
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, KsqlConfig.KSQL_SERVICE_ID_DEFAULT);
  }

  private void addFileProperties(Properties properties) throws IOException {
    if (propertiesFile != null) {
      try(final FileInputStream inputStream = new FileInputStream(propertiesFile)) {
        properties.load(inputStream);
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
