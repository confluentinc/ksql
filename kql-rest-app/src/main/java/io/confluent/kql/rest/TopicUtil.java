/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.rest;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TopicUtil {
  private static final Logger log = LoggerFactory.getLogger(TopicUtil.class);

  private final String zookeeperConnections;
  private final ZkUtils zkUtils;

  public TopicUtil(String zookeeperConnections) {
    this.zookeeperConnections = zookeeperConnections;

    // Could make this configurable, but hopefully by the time KS?QL is released Kafka will support topic management
    // without interacting with ZooKeeper at all and this whole class will either be gone completely different
    ZkClient zkClient = new ZkClient(
        zookeeperConnections,
        10000,
        8000,
        ZKStringSerializer$.MODULE$
    );
    zkUtils = new ZkUtils(
        zkClient,
        new ZkConnection(zookeeperConnections),
        false
    );
  }

  public void ensureTopicExists(String topic) {
    log.info(String.format(
        "Checking for existence of topic %s with Zookeeper connection(s) %s",
        topic,
        zookeeperConnections
    ));
    if (!AdminUtils.topicExists(zkUtils, topic)) {
      log.info(String.format(
          "Creating topic %s with Zookeeper connection(s) %s",
          topic,
          zookeeperConnections
      ));
      try {
        AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
      } catch (TopicExistsException exception) {
        log.warn(String.format(
            "Attempted to create topic %s with Zookeeper connection(s) %s that already existed",
            topic,
            zookeeperConnections
        ));
      }
    }
  }
}