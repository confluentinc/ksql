package io.confluent.ksql.physical.scalablepush;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;

public class TokenUtils {

  public static String getToken(Map<TopicPartition, Long> offsets, String topic, int numPartitions) {
    List<String> offsetList = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      TopicPartition tp = new TopicPartition(topic, i);
      offsetList.add(Long.toString(offsets.containsKey(tp) ? offsets.get(tp) : 0));
    }
    return String.join(",", offsetList);
  }

  public static String getToken(Map<Integer, Long> offsets) {
    int numPartitions = offsets.keySet().stream().reduce(0, Math::max) + 1;
    List<String> offsetList = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      offsetList.add(Long.toString(offsets.containsKey(i) ? offsets.get(i) : 0));
    }
    return String.join(",", offsetList);
  }

  public static Map<Integer, Long> parseToken(Optional<String> token) {
    if (token.isPresent()) {
      int i = 0;
      Map<Integer, Long> offsets = new HashMap<>();
      for (String str : token.get().split(",")) {
        offsets.put(i++, Long.parseLong(str));
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
    for (Integer partition : currentOffsets.keySet()) {
      final long startOffset = startUpdatedOffsets.get(partition);
      final long currentOffset = currentOffsets.get(partition);
      if (startOffset > 0 && currentOffset > 0) {
        // Since current is inclusive, the next offset should start at current + 1
        if (startOffset > currentOffset + 1) {
          return true;
        }
      }
    }
    return false;
  }

  public static Map<Integer, Long> merge(
      final Map<Integer, Long> currentOffsets,
      final Optional<String> token
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
