/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.zookeeper.ZooDefs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.util.KsqlException;

public class DdlUtil {

  private static final String ZK_TOPIC_PATH = "/brokers/topics";
  private static final String ZK_BROKER_PATH = "/brokers/ids";
  private static final String ZK_DELETE_TOPIC_PATH = "/admin/delete_topics";
  private static final String ZK_ENTITY_CONFIG_PATH = "/config/topics";

  public void createTopic(String topicName, int numPartitions, int replicationFactor) {
    // DO nothing!
  }

  public void deleteTopic(String topic) throws ZkNodeExistsException {

    ZkClient zkClient = new ZkClient("127.0.0.1:2181", 30 * 1000, 30 * 1000);
    zkClient.createPersistent(ZK_DELETE_TOPIC_PATH + "/" + topic, "", ZooDefs.Ids.OPEN_ACL_UNSAFE);

  }
}
