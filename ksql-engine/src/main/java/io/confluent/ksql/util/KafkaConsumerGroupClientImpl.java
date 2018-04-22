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

package io.confluent.ksql.util;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import kafka.admin.AdminClient;
import kafka.admin.ConsumerGroupCommand;

/**
 * Acts as a ConsumerGroup facade over the scala layer
 * Note: This functionality will very shortly be added to the java admin client, maybe even in
 * the upcoming 1.1. release: https://cwiki.apache.org/confluence/pages/viewpage
 * .action?pageId=74686265
 * Also, the Scala admin client is on the path to deprecation. See issue #642
 */
public class KafkaConsumerGroupClientImpl implements KafkaConsumerGroupClient {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumerGroupClientImpl.class);
  private static final int ADMIN_CLIENT_TIMEOUT_MS = 1000;
  private final AdminClient adminClient;
  private final KsqlConfig ksqlConfig;

  public KafkaConsumerGroupClientImpl(KsqlConfig ksqlConfig) {
    this.ksqlConfig = ksqlConfig;
    Properties props = new Properties();
    props.putAll(ksqlConfig.getKsqlAdminClientConfigProps());
    this.adminClient = AdminClient.create(props);
  }

  @Override
  public List<String> listGroups() {

    String[] args = consumerGroupCommandOptions();

    ConsumerGroupCommand.ConsumerGroupCommandOptions opts =
        new ConsumerGroupCommand.ConsumerGroupCommandOptions(args);
    ConsumerGroupCommand.KafkaConsumerGroupService consumerGroupService =
        new ConsumerGroupCommand.KafkaConsumerGroupService(opts);
    scala.collection.immutable.List<String> consumerGroups = consumerGroupService.listGroups();
    scala.collection.Iterator<String> consumerGroupsIterator = consumerGroups.iterator();
    ArrayList<String> results = new ArrayList<>();
    while (consumerGroupsIterator.hasNext()) {
      results.add(consumerGroupsIterator.next());
    }
    return results;
  }

  private String[] consumerGroupCommandOptions() {
    // The ConsumerGroupCommand we use instantiates its own admin client. However, the configs
    // for the underlying admin client can be passed only through a properties file. So we dump
    // the admin client configs to a temporary file and then use that file to configure the
    // underlying admin client correctly.
    Map<String, Object> clientConfigProps = ksqlConfig.getKsqlAdminClientConfigProps();
    try {
      File tmpConfigFile = flushPropertiesToTempFile(clientConfigProps);
      String[] args = {
          "--bootstrap-server", (String) clientConfigProps.get("bootstrap.servers"),
          "--command-config", tmpConfigFile.getAbsolutePath()
      };
      return args;
    } catch (IOException e) {
      log.error("Could not configure the list groups command.", e);
      throw new KsqlException("Could not list groups", e);
    }
  }

  private File flushPropertiesToTempFile(Map<String, Object> configProps) throws IOException {
    FileAttribute<Set<PosixFilePermission>> attributes
        = PosixFilePermissions.asFileAttribute(new HashSet<>(
            Arrays.asList(PosixFilePermission.OWNER_WRITE,
                          PosixFilePermission.OWNER_READ)));
    File configFile = Files.createTempFile("ksqlclient", "properties", attributes).toFile();
    configFile.deleteOnExit();

    try (FileOutputStream outputStream = new FileOutputStream(configFile)) {
      Properties clientProps = new Properties();
      for (Map.Entry<String, Object> property
          : configProps.entrySet()) {
        clientProps.put(property.getKey(), property.getValue());
      }
      clientProps.store(outputStream, "Configuration properties of KSQL AdminClient");
    }
    return configFile;
  }

  @Override
  public void close() {
    adminClient.close();
  }

  public ConsumerGroupSummary describeConsumerGroup(String group) {

    AdminClient.ConsumerGroupSummary consumerGroupSummary = adminClient.describeConsumerGroup(
        group,
        ADMIN_CLIENT_TIMEOUT_MS
    );
    scala.collection.immutable.List<AdminClient.ConsumerSummary> consumerSummaryList =
        consumerGroupSummary.consumers().get();
    scala.collection.Iterator<AdminClient.ConsumerSummary> consumerSummaryIterator =
        consumerSummaryList.iterator();

    ConsumerGroupSummary results = new ConsumerGroupSummary();

    while (consumerSummaryIterator.hasNext()) {
      AdminClient.ConsumerSummary consumerSummary = consumerSummaryIterator.next();

      ConsumerSummary consumerSummary1 = new ConsumerSummary(consumerSummary.consumerId());
      results.addConsumerSummary(consumerSummary1);

      scala.collection.immutable.List<TopicPartition> topicPartitionList =
          consumerSummary.assignment();
      scala.collection.Iterator<TopicPartition> topicPartitionIterator =
          topicPartitionList.iterator();

      while (topicPartitionIterator.hasNext()) {
        TopicPartition topicPartition = topicPartitionIterator.next();
        consumerSummary1.addPartition(new TopicPartition(
            topicPartition.topic(),
            topicPartition.partition()
        ));
      }
    }
    return results;
  }
}
