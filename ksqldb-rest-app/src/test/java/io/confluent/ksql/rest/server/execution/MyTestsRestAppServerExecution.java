package io.confluent.ksql.rest.server.execution;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        AssertSchemaExecutorTest.class,
        AssertTopicExecutorTest.class,
        ConnectExecutorTest.class,
        DefaultCommandQueueSyncTest.class,
        DescribeConnectorExecutorTest.class,
        DescribeFunctionExecutorTest.class,
        DropConnectorExecutorTest.class,
        ExplainExecutorTest.class,
        InsertValuesExecutorTest.class,
        ListConnectorPluginsTest.class,
        ListConnectorsExecutorTest.class,
        ListFunctionsExecutorTest.class,
        ListPropertiesExecutorTest.class,
        ListQueriesExecutorTest.class,
        ListSourceExecutorTest.class,
        ListTopicsExecutorTest.class,
        ListTypesExecutorTest.class,
        ListVariablesExecutorTest.class,
        PropertyExecutorTest.class,
        PullQueryMetricsTest.class,
        RemoteHostExecutorTest.class,
        RemoteSourceDescriptionExecutorTest.class,
        RequestHandlerTest.class,
        ScalablePushQueryMetricsTest.class,
        TerminateQueryExecutorTest.class,
        VariableExecutorTest.class,
})
public class MyTestsRestAppServerExecution {
}
