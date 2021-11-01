package io.confluent.ksql.physical.scalablepush;

import java.util.List;
import java.util.Optional;

public interface PushPhysicalPlanCreator {
  PushPhysicalPlan create(Optional<List<Long>> token);
}
