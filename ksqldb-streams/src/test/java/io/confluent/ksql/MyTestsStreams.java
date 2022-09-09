package io.confluent.ksql;

import io.confluent.ksql.execution.streams.AggregateParamsFactoryTest;
import io.confluent.ksql.execution.streams.ForeignKeyJoinParamsFactoryTest;
import io.confluent.ksql.execution.streams.ForeignKeyTableTableJoinBuilderTest;
import io.confluent.ksql.execution.streams.GroupByParamsFactoryTest;
import io.confluent.ksql.execution.streams.GroupByParamsV1FactoryTest;
import io.confluent.ksql.execution.streams.ImmutabilityTest;
import io.confluent.ksql.execution.streams.JoinParamsFactoryTest;
import io.confluent.ksql.execution.streams.KsqlValueJoinerTest;
import io.confluent.ksql.execution.streams.PartitionByParamsFactoryTest;
import io.confluent.ksql.execution.streams.PlanInfoExtractorTest;
import io.confluent.ksql.execution.streams.RegisterSchemaCallbackTest;
import io.confluent.ksql.execution.streams.SinkBuilderTest;
import io.confluent.ksql.execution.streams.SourceBuilderTest;
import io.confluent.ksql.execution.streams.SourceBuilderV1Test;
import io.confluent.ksql.execution.streams.StepSchemaResolverTest;
import io.confluent.ksql.execution.streams.StreamAggregateBuilderTest;
import io.confluent.ksql.execution.streams.StreamFilterBuilderTest;
import io.confluent.ksql.execution.streams.StreamGroupByBuilderTest;
import io.confluent.ksql.execution.streams.StreamGroupByBuilderV1Test;
import io.confluent.ksql.execution.streams.StreamSelectBuilderTest;
import io.confluent.ksql.execution.streams.StreamSelectKeyBuilderTest;
import io.confluent.ksql.execution.streams.StreamSelectKeyBuilderV1Test;
import io.confluent.ksql.execution.streams.StreamStreamJoinBuilderTest;
import io.confluent.ksql.execution.streams.StreamTableJoinBuilderTest;
import io.confluent.ksql.execution.streams.TableAggregateBuilderTest;
import io.confluent.ksql.execution.streams.TableFilterBuilderTest;
import io.confluent.ksql.execution.streams.TableGroupByBuilderTest;
import io.confluent.ksql.execution.streams.TableGroupByBuilderV1Test;
import io.confluent.ksql.execution.streams.TableSuppressBuilderTest;
import io.confluent.ksql.execution.streams.TableTableJoinBuilderTest;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterializationFactoryTest;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterializationTest;
import io.confluent.ksql.execution.streams.materialization.RowTest;
import io.confluent.ksql.execution.streams.materialization.TableRowValidationTest;
import io.confluent.ksql.execution.streams.materialization.WindowTest;
import io.confluent.ksql.execution.streams.materialization.WindowedRowTest;
import io.confluent.ksql.execution.streams.materialization.ks.KsLocatorTest;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializationFactoryTest;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializationTest;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializedSessionTableIQv2Test;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializedSessionTableTest;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializedTableTest;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializedWindowTableIQv2Test;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializedWindowTableTest;
import io.confluent.ksql.execution.streams.materialization.ks.KsStateStoreTest;
import io.confluent.ksql.execution.streams.materialization.ks.SessionStoreCacheBypassTest;
import io.confluent.ksql.execution.streams.materialization.ks.WindowStoreCacheBypassTest;
import io.confluent.ksql.execution.streams.metrics.RocksDBMetricsCollectorTest;
import io.confluent.ksql.execution.streams.timestamp.KsqlTimestampExtractorTest;
import io.confluent.ksql.execution.streams.timestamp.LoggingTimestampExtractorTest;
import io.confluent.ksql.execution.streams.timestamp.LongColumnTimestampExtractionPolicyTest;
import io.confluent.ksql.execution.streams.timestamp.MetadataTimestampExtractionPolicyTest;
import io.confluent.ksql.execution.streams.timestamp.MetadataTimestampExtractorTest;
import io.confluent.ksql.execution.streams.timestamp.StringTimestampExtractionPolicyTest;
import io.confluent.ksql.execution.streams.timestamp.StringTimestampExtractorTest;
import io.confluent.ksql.execution.streams.timestamp.TimestampColumnExtractorsTest;
import io.confluent.ksql.execution.streams.timestamp.TimestampColumnTimestampExtractionPolicyTest;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicyFactoryTest;
import io.confluent.ksql.execution.streams.transform.KsTransformerTest;
import io.confluent.ksql.execution.streams.transform.KsValueTransformerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        KsLocatorTest.class,
        KsMaterializationFactoryTest.class,
        KsMaterializationTest.class,
        KsMaterializedSessionTableIQv2Test.class,
        KsMaterializedSessionTableTest.class,
        KsMaterializedSessionTableIQv2Test.class,
        KsMaterializedTableTest.class,
        KsMaterializedWindowTableIQv2Test.class,
        KsMaterializedWindowTableTest.class,
        KsStateStoreTest.class,
        SessionStoreCacheBypassTest.class,
        WindowStoreCacheBypassTest.class,
        KsqlMaterializationFactoryTest.class,
        KsqlMaterializationTest.class,
        RowTest.class,
        TableRowValidationTest.class,
        WindowedRowTest.class,
        WindowTest.class,
        RocksDBMetricsCollectorTest.class,
        KsqlTimestampExtractorTest.class,
        LoggingTimestampExtractorTest.class,
        LongColumnTimestampExtractionPolicyTest.class,
        MetadataTimestampExtractionPolicyTest.class,
        MetadataTimestampExtractorTest.class,
        StringTimestampExtractionPolicyTest.class,
        StringTimestampExtractorTest.class,
        TimestampColumnExtractorsTest.class,
        TimestampColumnTimestampExtractionPolicyTest.class,
        TimestampExtractionPolicyFactoryTest.class,
        KsTransformerTest.class,
        KsValueTransformerTest.class,
        AggregateParamsFactoryTest.class,
        ForeignKeyJoinParamsFactoryTest.class,
        ForeignKeyTableTableJoinBuilderTest.class,
        GroupByParamsFactoryTest.class,
        GroupByParamsV1FactoryTest.class,
        ImmutabilityTest.class,
        JoinParamsFactoryTest.class,
        KsqlValueJoinerTest.class,
        PartitionByParamsFactoryTest.class,
        PlanInfoExtractorTest.class,
        RegisterSchemaCallbackTest.class,
        SinkBuilderTest.class,
        SourceBuilderTest.class,
        SourceBuilderV1Test.class,
        StepSchemaResolverTest.class,
        StreamAggregateBuilderTest.class,
        StreamFilterBuilderTest.class,
        StreamGroupByBuilderTest.class,
        StreamGroupByBuilderV1Test.class,
        StreamSelectBuilderTest.class,
        StreamSelectKeyBuilderTest.class,
        StreamSelectKeyBuilderV1Test.class,
        StreamStreamJoinBuilderTest.class,
        StreamTableJoinBuilderTest.class,
        TableAggregateBuilderTest.class,
        TableFilterBuilderTest.class,
        TableGroupByBuilderTest.class,
        TableGroupByBuilderV1Test.class,
        TableSuppressBuilderTest.class,
        TableTableJoinBuilderTest.class
})
public class MyTestsStreams {
}
