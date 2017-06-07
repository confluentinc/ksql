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

public class DdlUtil {

  private static final String ZK_TOPIC_PATH = "/brokers/topics";
  private static final String ZK_BROKER_PATH = "/brokers/ids";
  private static final String ZK_DELETE_TOPIC_PATH = "/admin/delete_topics";
  private static final String ZK_ENTITY_CONFIG_PATH = "/config/topics";

  public void createTopic(String topicName, int numPartitions, int replicationFactor) {

    ZkClient zkClient = new ZkClient("127.0.0.1:2181", 30 * 1000, 30 * 1000);
    ObjectMapper mapper = new ObjectMapper();

    List<Integer> brokers = getBrokers(zkClient);
    int numBrokers = brokers.size();
    if (numBrokers < replicationFactor) {
      replicationFactor = numBrokers;
    }

    Map<Integer, List<Integer>> assignment = new HashMap<>();

    for (int i = 0; i < numPartitions; i++) {
      ArrayList<Integer> brokerList = new ArrayList<>();
      for (int r = 0; r < replicationFactor; r++) {
        int shift = r * numBrokers / replicationFactor;
        brokerList.add(brokers.get((i + shift) % numBrokers));
      }
      assignment.put(i, brokerList);
    }
    // write out config first just like in AdminUtils.scala createOrUpdateTopicPartitionAssignmentPathInZK()
    try {
      Map<String, Object> dataMap = new HashMap<>();
      dataMap.put("version", 1);
      dataMap.put("config", 1000);
      String data = mapper.writeValueAsString(dataMap);
      zkClient.createPersistent(ZK_ENTITY_CONFIG_PATH + "/" + topicName, data,
                                ZooDefs.Ids.OPEN_ACL_UNSAFE);
    } catch (JsonProcessingException e) {
      throw new StreamsException(
          "Error while creating topic config in ZK for internal topic " + topicName, e);
    }

    // try to write to ZK with open ACL
    try {
      Map<String, Object> dataMap = new HashMap<>();
      dataMap.put("version", 1);
      dataMap.put("partitions", assignment);
      String data = mapper.writeValueAsString(dataMap);

      zkClient.createPersistent(ZK_TOPIC_PATH + "/" + topicName, data, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    } catch (JsonProcessingException e) {
      throw new StreamsException(
          "Error while creating topic metadata in ZK for internal topic " + topicName, e);
    }

  }

  private List<Integer> getBrokers(ZkClient zkClient) {
    List<Integer> brokers = new ArrayList<>();
    for (String broker : zkClient.getChildren(ZK_BROKER_PATH)) {
      brokers.add(Integer.parseInt(broker));
    }
    Collections.sort(brokers);

    return brokers;
  }

  public void deleteTopic(String topic) throws ZkNodeExistsException {

    ZkClient zkClient = new ZkClient("127.0.0.1:2181", 30 * 1000, 30 * 1000);
    zkClient.createPersistent(ZK_DELETE_TOPIC_PATH + "/" + topic, "", ZooDefs.Ids.OPEN_ACL_UNSAFE);

  }

  public static void main(String[] args) {
    new DdlUtil().deleteTopic("TestTopic");
  }
}
