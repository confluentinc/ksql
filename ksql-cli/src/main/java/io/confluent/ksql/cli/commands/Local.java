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

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Port;
import com.github.rvesse.airline.annotations.restrictions.PortType;

import org.apache.kafka.streams.StreamsConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import io.confluent.ksql.cli.LocalCli;
import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.cli.console.JLineTerminal;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.server.KsqlRestApplication;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.util.CliUtils;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;

@Command(name = "local", description = "Run a local (standalone) Cli session")
public class Local extends AbstractCliCommands {


  private static final String PROPERTIES_FILE_OPTION_NAME = "--properties-file";

  private static final String PORT_NUMBER_OPTION_NAME = "--port-number";
  private static final int PORT_NUMBER_OPTION_DEFAULT = 9098;

  private static final String KAFKA_BOOTSTRAP_SERVER_OPTION_NAME = "--bootstrap-server";
  private static final String KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT = "localhost:9092";

  private static final String SERVICE_ID_OPTION_NAME = "--service-id";
  private static final String SERVICE_ID_OPTION_DEFAULT = KsqlConfig.KSQL_SERVICE_ID_DEFAULT;

  private static final String COMMAND_TOPIC_SUFFIX_OPTION_NAME = "--command-topic-suffix";
  private static final String COMMAND_TOPIC_SUFFIX_OPTION_DEFAULT = "commands";

  private static final String SCHEMA_REGISTRY_URL_OPTION_NAME = "--schema-registry-url";
  private static final String SCHEMA_REGISTRY_URL_OPTION_DEFAULT = "http://localhost:8081";

  @Port(acceptablePorts = PortType.ANY)
  @Option(
      name = PORT_NUMBER_OPTION_NAME,
      description = "The portNumber to use for the connection (defaults to "
                    + PORT_NUMBER_OPTION_DEFAULT
                    + ")"
  )
  int portNumber = PORT_NUMBER_OPTION_DEFAULT;

  @Option(
      name = KAFKA_BOOTSTRAP_SERVER_OPTION_NAME,
      description = "The Kafka server to connect to (defaults to "
                    + KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT
                    + ")"
  )
  String bootstrapServer;

  @Option(
      name = SERVICE_ID_OPTION_NAME,
      description = "The application ID to use for the created Kafka Streams instance(s) "
                    + "(defaults to '"
                    + SERVICE_ID_OPTION_DEFAULT
                    + "')"
  )
  String serviceId;

  @Option(
      name = COMMAND_TOPIC_SUFFIX_OPTION_NAME,
      description = "The suffix to append to the end of the name of the command topic "
                    + "(defaults to '"
                    + COMMAND_TOPIC_SUFFIX_OPTION_DEFAULT
                    + "')"
  )
  String commandTopicSuffix;

  @Option(
      name = PROPERTIES_FILE_OPTION_NAME,
      description = "A file specifying properties for Ksql and its underlying Kafka Streams "
                    + "instance(s) (can specify port number, bootstrap server, etc. but these "
                    + "options will "
                    + "be overridden if also given via  flags)"
  )
  String propertiesFile;

  @Option(
      name = SCHEMA_REGISTRY_URL_OPTION_NAME,
      description = "The url of the schema registry server to be used. It defaults to " +
                    SCHEMA_REGISTRY_URL_OPTION_DEFAULT + ". Avro support requires a functioning"
                    + " schema registry which the KSQL instance can connect to. Setting this will"
                    + " override the schema registry url in the properties file, if set."
  )
  String schemaRegistryUrl;

  @Override
  public LocalCli getCli() throws Exception {
    Properties serverProperties;
    try {
      serverProperties = getStandaloneProperties();
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }

    KsqlRestClient restClient = new KsqlRestClient(CliUtils.getLocalServerAddress(portNumber));
    Console terminal = new JLineTerminal(parseOutputFormat(), restClient);
    terminal.writer().println("Initializing KSQL...");
    terminal.flush();
    // Have to override listeners config to make sure it aligns with port number for client
    serverProperties.put(
        KsqlRestConfig.LISTENERS_CONFIG,
        CliUtils.getLocalServerAddress(portNumber)
    );
    KsqlRestConfig restServerConfig = new KsqlRestConfig(serverProperties);
    KsqlRestApplication restServer = KsqlRestApplication.buildApplication(
        restServerConfig,
        false,
        new KsqlVersionCheckerAgent()
    );
    restServer.start();

    versionCheckerAgent.start(KsqlModuleType.LOCAL_CLI, serverProperties);
    return new LocalCli(
        streamedQueryRowLimit,
        streamedQueryTimeoutMs,
        restClient,
        terminal,
        restServer
    );
  }

  private Properties getStandaloneProperties() throws IOException {
    Properties properties = new Properties();
    addDefaultProperties(properties);
    addFileProperties(properties);
    addFlagProperties(properties);
    return properties;
  }

  private void addDefaultProperties(Properties properties) {
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT);
    properties.put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, SERVICE_ID_OPTION_DEFAULT);
    properties.put(
        KsqlRestConfig.COMMAND_TOPIC_SUFFIX_CONFIG,
        COMMAND_TOPIC_SUFFIX_OPTION_DEFAULT
    );
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, KsqlConfig.KSQL_SERVICE_ID_DEFAULT);
    properties.put(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, SCHEMA_REGISTRY_URL_OPTION_DEFAULT);
  }

  private void addFileProperties(Properties properties) throws IOException {
    if (propertiesFile != null) {
      try (final FileInputStream input = new FileInputStream(propertiesFile)) {
        properties.load(input);
      }

      if (properties.containsKey(KsqlConfig.KSQL_SERVICE_ID_CONFIG)) {
        properties.put(
            StreamsConfig.APPLICATION_ID_CONFIG,
            properties.getProperty(KsqlConfig.KSQL_SERVICE_ID_CONFIG)
        );
      } else {
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, KsqlConfig.KSQL_SERVICE_ID_DEFAULT);
      }
    }
  }

  private void addFlagProperties(Properties properties) {
    if (bootstrapServer != null) {
      properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    }
    if (serviceId != null) {
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, serviceId);
      properties.put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, serviceId);
    }
    if (commandTopicSuffix != null) {
      properties.put(KsqlRestConfig.COMMAND_TOPIC_SUFFIX_CONFIG, commandTopicSuffix);
    }

    if (schemaRegistryUrl != null) {
      properties.put(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, schemaRegistryUrl);
    }
  }
}
