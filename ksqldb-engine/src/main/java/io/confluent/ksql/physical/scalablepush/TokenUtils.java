package io.confluent.ksql.physical.scalablepush;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;

public class TokenUtils {

  public static List<Long> getToken(Map<TopicPartition, Long> offsets, String topic, int numPartitions) {
    List<Long> offsetList = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      TopicPartition tp = new TopicPartition(topic, i);
      offsetList.add(offsets.getOrDefault(tp, 0L));
    }
    return ImmutableList.copyOf(offsetList);
  }

  public static List<Long> getToken(Map<Integer, Long> offsets) {
    int numPartitions = offsets.keySet().stream().reduce(0, Math::max) + 1;
    List<Long> offsetList = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      offsetList.add(offsets.getOrDefault(i, 0L));
    }
    return ImmutableList.copyOf(offsetList);
  }

  public static Map<Integer, Long> parseToken(Optional<List<Long>> token) {
    if (token.isPresent()) {
      int i = 0;
      Map<Integer, Long> offsets = new HashMap<>();
      for (Long offset : token.get()) {
        offsets.put(i++, offset);
      }
      return offsets;
    } else {
      return Collections.emptyMap();
    }
  }

  public static boolean gapExists(
      final Map<Integer, Long> currentOffsets,
      final Map<Integer, Long> startUpdatedOffsets
  ) {
    System.out.println("Checking for gap between current " + currentOffsets + " update " + startUpdatedOffsets);
    for (Integer partition : currentOffsets.keySet()) {
      final long startOffset = startUpdatedOffsets.get(partition);
      final long currentOffset = currentOffsets.get(partition);
      if (startOffset > 0 && currentOffset > 0) {
        // Since current is inclusive, the next offset should start at current + 1
        if (startOffset > currentOffset) {
          System.out.println("Found gap");
          return true;
        }
      }
    }
    System.out.println("No gap");
    return false;
  }

  public static Map<Integer, Long> merge(
      final Map<Integer, Long> currentOffsets,
      final Optional<List<Long>> token
  ) {
    if (token.isPresent()) {
      ImmutableMap.Builder<Integer, Long> builder = ImmutableMap.builder();
      Map<Integer, Long> offsets = parseToken(token);
      for (Integer partition : currentOffsets.keySet()) {
        if (offsets.get(partition) > 0) {
          builder.put(partition, offsets.get(partition));
        } else {
          builder.put(partition, currentOffsets.get(partition));
        }
      }
      return builder.build();
    } else {
      return Collections.emptyMap();
    }
  }
}
