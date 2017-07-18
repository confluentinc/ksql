/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.util;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface KafkaTopicClient extends Closeable {

  public boolean ensureTopicExists(String topic, int numPartitions, short replicatonFactor);

  public boolean createTopic(String topic, int numPartitions, short replicatonFactor);

  public Set<String> listTopicNames() throws ExecutionException, InterruptedException;

}
