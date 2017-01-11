package io.confluent.kql.physical;


import io.confluent.kql.planner.plan.PlanVisitor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStreamBuilder;

public class PhysicalPlanBuilderVisitor extends PlanVisitor {

  static Serde<GenericRow> genericRowSerde = null;
  KStreamBuilder builder;

  public PhysicalPlanBuilderVisitor(KStreamBuilder builder) {
    this.builder = builder;
  }


}
