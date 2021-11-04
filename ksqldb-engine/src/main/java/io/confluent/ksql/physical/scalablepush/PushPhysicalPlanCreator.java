package io.confluent.ksql.physical.scalablepush;

import io.confluent.ksql.util.PushOffsetRange;
import java.util.Optional;

public interface PushPhysicalPlanCreator {
  PushPhysicalPlan create(Optional<PushOffsetRange> offsetRange,
      Optional<String> catchupConsumerGroup);
}
