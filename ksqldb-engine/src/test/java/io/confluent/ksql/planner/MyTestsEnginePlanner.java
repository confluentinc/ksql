package io.confluent.ksql.planner;

import io.confluent.ksql.planner.plan.AggregateNodeTest;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlanTest;
import io.confluent.ksql.planner.plan.DataSourceNodeTest;
import io.confluent.ksql.planner.plan.FilterNodeTest;
import io.confluent.ksql.planner.plan.FinalProjectNodeTest;
import io.confluent.ksql.planner.plan.FlatMapNodeTest;
import io.confluent.ksql.planner.plan.ImplicitlyCastResolverTest;
import io.confluent.ksql.planner.plan.JoinNodeTest;
import io.confluent.ksql.planner.plan.KsqlBareOutputNodeTest;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNodeTest;
import io.confluent.ksql.planner.plan.LogicRewriterTest;
import io.confluent.ksql.planner.plan.PlanBuildContextTest;
import io.confluent.ksql.planner.plan.PlanNodeTest;
import io.confluent.ksql.planner.plan.PreJoinProjectNodeTest;
import io.confluent.ksql.planner.plan.ProjectNodeTest;
import io.confluent.ksql.planner.plan.PullQueryRewriterTest;
import io.confluent.ksql.planner.plan.QueryFilterNodeTest;
import io.confluent.ksql.planner.plan.QueryProjectNodeTest;
import io.confluent.ksql.planner.plan.SuppressNodeTest;
import io.confluent.ksql.planner.plan.UserRepartitionNodeTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        AggregateNodeTest.class,
        ConfiguredKsqlPlanTest.class,
        DataSourceNodeTest.class,
        FilterNodeTest.class,
        FinalProjectNodeTest.class,
        FlatMapNodeTest.class,
        ImplicitlyCastResolverTest.class,
        JoinNodeTest.class,
        KsqlBareOutputNodeTest.class,
        KsqlStructuredDataOutputNodeTest.class,
        LogicRewriterTest.class,
        PlanBuildContextTest.class,
        PlanNodeTest.class,
        PreJoinProjectNodeTest.class,
        ProjectNodeTest.class,
        PullQueryRewriterTest.class,
        QueryFilterNodeTest.class,
        QueryProjectNodeTest.class,
        SuppressNodeTest.class,
        UserRepartitionNodeTest.class,
        JoinTreeTest.class,
        LogicalPlannerTest.class,
        ProjectionTest.class,
        RequiredColumnsTest.class,
})
public class MyTestsEnginePlanner {
}
