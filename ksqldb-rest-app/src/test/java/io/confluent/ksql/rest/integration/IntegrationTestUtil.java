package io.confluent.ksql.rest.integration;

import com.google.common.base.Preconditions;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Source;
import org.apache.kafka.streams.TopologyDescription.Subtopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestUtil.class);
  private static final Duration MAX_QUERY_RUNNING_CHECK = Duration.ofSeconds(45);
  private static final Duration MAX_STATIC_WARM_UP = Duration.ofSeconds(45);
  private static final Duration MAX_TOPIC_NAME_LOOKUP = Duration.ofSeconds(45);

  /**
   * This method looks at all of the source topics that feed into all of the subtopologies and waits
   * for the consumer group associated with each application to reach the end offsets for those
   * topics.  This effectively ensures that nothing is lagging when it completes successfully.
   * This should ensure that any materialized state has been built up correctly and is ready for
   * pull queries.
   */
  public static void waitForPersistentQueriesToProcessInputs(
      final EmbeddedSingleNodeKafkaCluster kafkaCluster,
      final KsqlExecutionContext engine
  ) {
    // First wait for the queries to be in the RUNNING state
    boolean allRunning = false;
    final long queryRunningThreshold = System.currentTimeMillis()
        + MAX_QUERY_RUNNING_CHECK.toMillis();
    while (System.currentTimeMillis() < queryRunningThreshold) {
      boolean notReady = false;
      for (PersistentQueryMetadata persistentQueryMetadata : engine.getPersistentQueries()) {
        if (persistentQueryMetadata.getState() != State.RUNNING) {
          LOG.info("Not all persistent queries are running yet");
          notReady = true;
        }
      }
      if (notReady) {
        threadYield();
      } else {
        allRunning = true;
        LOG.info("All persistent queries are now running");
        break;
      }
    }
    if (!allRunning) {
      throw new AssertionError("Timed out while trying to wait for queries to begin running");
    }

    // Collect all application ids
    List<String> queryApplicationIds = engine.getPersistentQueries().stream()
        .map(QueryMetadata::getQueryApplicationId)
        .collect(Collectors.toList());

    // Collect all possible source topic names for each application id
    Map<String, Set<String>> possibleTopicNamesByAppId = engine.getPersistentQueries().stream()
        .collect(Collectors.toMap(
            QueryMetadata::getQueryApplicationId,
            m -> {
              Set<String> topics = getSourceTopics(m);
              Set<String> possibleInternalNames = topics.stream()
                  .map(t -> m.getQueryApplicationId() + "-" + t)
                  .collect(Collectors.toSet());
              Set<String> all = new HashSet<>();
              all.addAll(topics);
              all.addAll(possibleInternalNames);
              return all;
            }
        ));
    final Set<String> possibleTopicNames = possibleTopicNamesByAppId.values()
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
    // Every topic is either internal or not, so we expect to match exactly half of them.
    int expectedTopics = possibleTopicNames.size() / 2;

    // Find the intersection of possible topic names and real topic names, and wait until the
    // expected number are all there
    final Set<String> topics = new HashSet<>();
    boolean foundTopics = false;
    final long topicThreshold = System.currentTimeMillis() + MAX_TOPIC_NAME_LOOKUP.toMillis();
    while (System.currentTimeMillis() < topicThreshold) {
      Set<String> expectedNames = kafkaCluster.
          getTopics();
      expectedNames.retainAll(possibleTopicNames);
      if (expectedNames.size() == expectedTopics) {
        foundTopics = true;
        topics.addAll(expectedNames);
        LOG.info("All expected topics have now been found");
        break;
      }
    }
    if (!foundTopics) {
      throw new AssertionError("Timed out while trying to find topics");
    }

    // Only retain topic names which are known to exist.
    Map<String, Set<String>> topicNamesByAppId = possibleTopicNamesByAppId.entrySet().stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            e -> {
              e.getValue().retainAll(topics);
              return e.getValue();
            }
        ));

    Map<String, Integer> partitionCount = kafkaCluster.getPartitionCount(topics);
    Map<String, List<TopicPartition>> topicPartitionsByAppId = queryApplicationIds.stream()
        .collect(Collectors.toMap(
            appId -> appId,
            appId -> {
              final List<TopicPartition> allTopicPartitions = new ArrayList<>();
              for (String topic : topicNamesByAppId.get(appId)) {
                for (int i = 0; i < partitionCount.get(topic); i++) {
                  final TopicPartition tp = new TopicPartition(topic, i);
                  allTopicPartitions.add(tp);
                }
              }
              return allTopicPartitions;
            }));

    final long threshold = System.currentTimeMillis() + MAX_STATIC_WARM_UP.toMillis();
    mainloop:
    while (System.currentTimeMillis() < threshold) {
      for (String queryApplicationId : queryApplicationIds) {
        final List<TopicPartition> topicPartitions
            = topicPartitionsByAppId.get(queryApplicationId);

        Map<TopicPartition, Long> currentOffsets =
            kafkaCluster.getConsumerGroupOffset(queryApplicationId);
        Map<TopicPartition, Long> endOffsets = kafkaCluster.getEndOffsets(topicPartitions,
            // Since we're doing At Least Once, we can do read uncommitted.
            IsolationLevel.READ_COMMITTED);

        for (final TopicPartition tp : topicPartitions) {
          if (!currentOffsets.containsKey(tp) && endOffsets.get(tp) > 0) {
            LOG.info("Haven't committed offsets yet for " + tp + " end offset " + endOffsets.get(tp));
            threadYield();
            continue mainloop;
          }
        }

        for (final Map.Entry<TopicPartition, Long> entry : currentOffsets.entrySet()) {
          final TopicPartition tp = entry.getKey();
          final long currentOffset = entry.getValue();
          final long endOffset = endOffsets.get(tp);
          if (currentOffset < endOffset) {
            LOG.info("Offsets are not caught up current: " + currentOffsets + " end: "
                + endOffsets);
            threadYield();
            continue mainloop;
          }
        }
      }
      LOG.info("Offsets are all up to date");
      return;
    }
    LOG.info("Timed out waiting for correct response");
    throw new AssertionError("Timed out while trying to wait for offsets");
  }

  private static Set<String> getSourceTopics(
      final PersistentQueryMetadata persistentQueryMetadata
  ) {
    Set<String> topics = new HashSet<>();
    for (final Subtopology subtopology :
        persistentQueryMetadata.getTopology().describe().subtopologies()) {
      for (final TopologyDescription.Node node : subtopology.nodes()) {
        if (node instanceof Source) {
          final Source source = (Source) node;
          Preconditions.checkNotNull(source.topicSet(), "Expecting topic set, not regex");
          topics.addAll(source.topicSet());
        }
      }
    }
    return topics;
  }

  private static void threadYield() {
    try {
      // More reliable than Thread.yield
      Thread.sleep(10);
    } catch (final InterruptedException e) {
      // ignore
    }
  }
}
