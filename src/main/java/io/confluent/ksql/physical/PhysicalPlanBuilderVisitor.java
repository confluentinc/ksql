package io.confluent.ksql.physical;


import io.confluent.ksql.planner.plan.PlanVisitor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStreamBuilder;

public class PhysicalPlanBuilderVisitor extends PlanVisitor {

    static Serde<GenericRow> genericRowSerde = null;
    KStreamBuilder builder;

    public PhysicalPlanBuilderVisitor(KStreamBuilder builder) {
        this.builder = builder;
    }



}
