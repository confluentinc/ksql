package io.confluent.ksql.execution;


import io.confluent.ksql.execution.common.operators.ProjectOperatorTest;
import io.confluent.ksql.execution.common.operators.SelectOperatorTest;
import io.confluent.ksql.execution.json.PlanJsonMapperTest;
import io.confluent.ksql.execution.plan.FormatsSerializationTest;
import io.confluent.ksql.execution.pull.HARoutingTest;
import io.confluent.ksql.execution.pull.operators.KeyedTableLookupOperatorTest;
import io.confluent.ksql.execution.pull.operators.KeyedWindowedTableLookupOperatorTest;
import io.confluent.ksql.execution.pull.operators.TableScanOperatorTest;
import io.confluent.ksql.execution.pull.operators.WindowedTableScanOperatorTest;
import io.confluent.ksql.execution.scalablepush.ProcessingQueueTest;
import io.confluent.ksql.execution.scalablepush.PushExecutionPlanBuilderTest;
import io.confluent.ksql.execution.scalablepush.PushExecutionPlanTest;
import io.confluent.ksql.execution.scalablepush.ScalablePushRegistryTest;
import io.confluent.ksql.execution.scalablepush.consumer.CatchupConsumerTest;
import io.confluent.ksql.execution.scalablepush.consumer.CatchupCoordinatorImplTest;
import io.confluent.ksql.execution.scalablepush.consumer.LatestConsumerTest;
import io.confluent.ksql.execution.scalablepush.consumer.ScalablePushConsumerTest;
import io.confluent.ksql.execution.scalablepush.locator.AllHostsLocatorTest;
import io.confluent.ksql.execution.scalablepush.operators.PeekStreamOperatorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ProjectOperatorTest.class,
        SelectOperatorTest.class,
        PlanJsonMapperTest.class,
        FormatsSerializationTest.class,
        KeyedTableLookupOperatorTest.class,
        KeyedWindowedTableLookupOperatorTest.class,
        TableScanOperatorTest.class,
        WindowedTableScanOperatorTest.class,
        HARoutingTest.class,
        CatchupConsumerTest.class,
        CatchupCoordinatorImplTest.class,
        LatestConsumerTest.class,
        ScalablePushConsumerTest.class,
        AllHostsLocatorTest.class,
        PeekStreamOperatorTest.class,
        ProcessingQueueTest.class,
        PushExecutionPlanBuilderTest.class,
        PushExecutionPlanTest.class,
        ScalablePushRegistryTest.class,
        ExecutionPlanBuilderTest.class,
        ExpressionEvaluatorParityTest.class,
})
public class MyTestsEngineExecution {
}
