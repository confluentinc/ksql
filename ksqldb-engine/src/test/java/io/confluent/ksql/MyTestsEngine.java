package io.confluent.ksql;

import io.confluent.ksql.analyzer.AggregateAnalyzerTest;
import io.confluent.ksql.analyzer.AnalysisTest;
import io.confluent.ksql.analyzer.AnalyzerFunctionalTest;
import io.confluent.ksql.analyzer.ColumnReferenceValidatorTest;
import io.confluent.ksql.analyzer.FilterTypeValidatorTest;
import io.confluent.ksql.analyzer.PullQueryValidatorTest;
import io.confluent.ksql.analyzer.PushQueryValidatorTest;
import io.confluent.ksql.analyzer.QueryAnalyzerFunctionalTest;
import io.confluent.ksql.analyzer.QueryAnalyzerTest;
import io.confluent.ksql.analyzer.QueryValidatorUtilTest;
import io.confluent.ksql.analyzer.RewrittenAnalysisTest;
import io.confluent.ksql.analyzer.SourceSchemasTest;
import io.confluent.ksql.connect.ConnectorTest;
import io.confluent.ksql.connect.supported.JdbcSourceTest;
import io.confluent.ksql.ddl.commands.AlterSourceFactoryTest;
import io.confluent.ksql.ddl.commands.CommandFactoriesTest;
import io.confluent.ksql.ddl.commands.CreateSourceFactoryTest;
import io.confluent.ksql.ddl.commands.DdlCommandExecTest;
import io.confluent.ksql.ddl.commands.DropSourceFactoryTest;
import io.confluent.ksql.ddl.commands.DropTypeFactoryTest;
import io.confluent.ksql.ddl.commands.RegisterTypeFactoryTest;
import io.confluent.ksql.embedded.KsqlContextTest;
import io.confluent.ksql.engine.ImmutabilityTest;
import io.confluent.ksql.engine.KsqlEngineTest;
import io.confluent.ksql.engine.KsqlPlanV1Test;
import io.confluent.ksql.engine.OrphanedTransientQueryCleanerTest;
import io.confluent.ksql.engine.QueryIdUtilTest;
import io.confluent.ksql.engine.QueryPlanTest;
import io.confluent.ksql.engine.RuntimeAssignorTest;
import io.confluent.ksql.engine.TransientQueryCleanupServiceTest;
import io.confluent.ksql.engine.generic.GenericExpressionResolverTest;
import io.confluent.ksql.engine.generic.GenericRecordFactoryTest;
import io.confluent.ksql.engine.rewrite.AstSanitizerTest;
import io.confluent.ksql.engine.rewrite.DataSourceExtractorTest;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriterTest;
import io.confluent.ksql.engine.rewrite.QueryAnonymizerTest;
import io.confluent.ksql.engine.rewrite.StatementRewriteForMagicPseudoTimestampTest;
import io.confluent.ksql.engine.rewrite.StatementRewriterTest;
import io.confluent.ksql.execution.ExecutionPlanBuilderTest;
import io.confluent.ksql.execution.ExpressionEvaluatorParityTest;
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
import io.confluent.ksql.format.DefaultFormatInjectorTest;
import io.confluent.ksql.function.udaf.array.CollectListUdafTest;
import io.confluent.ksql.function.udaf.array.CollectSetUdafTest;
import io.confluent.ksql.function.udaf.attr.AttrTest;
import io.confluent.ksql.function.udaf.average.AverageUdafTest;
import io.confluent.ksql.function.udaf.correlation.CorrelationUdafTest;
import io.confluent.ksql.function.udaf.count.CountDistinctKudafTest;
import io.confluent.ksql.function.udaf.count.CountKudafTest;
import io.confluent.ksql.function.udaf.map.HistogramUdafTest;
import io.confluent.ksql.function.udaf.max.BytesMaxKudafTest;
import io.confluent.ksql.function.udaf.max.DateMaxKudafTest;
import io.confluent.ksql.function.udaf.max.DecimalMaxKudafTest;
import io.confluent.ksql.function.udaf.max.DoubleMaxKudafTest;
import io.confluent.ksql.function.udaf.max.IntegerMaxKudafTest;
import io.confluent.ksql.function.udaf.max.LongMaxKudafTest;
import io.confluent.ksql.function.udaf.max.StringMaxKudafTest;
import io.confluent.ksql.function.udaf.max.TimeMaxKudafTest;
import io.confluent.ksql.function.udaf.max.TimestampMaxKudafTest;
import io.confluent.ksql.function.udaf.min.BytesMinKudafTest;
import io.confluent.ksql.function.udaf.min.DateMinKudafTest;
import io.confluent.ksql.function.udaf.min.DecimalMinKudafTest;
import io.confluent.ksql.function.udaf.min.DoubleMinKudafTest;
import io.confluent.ksql.function.udaf.min.IntegerMinKudafTest;
import io.confluent.ksql.function.udaf.min.LongMinKudafTest;
import io.confluent.ksql.function.udaf.min.StringMinKudafTest;
import io.confluent.ksql.function.udaf.min.TimeMinKudafTest;
import io.confluent.ksql.function.udaf.min.TimestampMinKudafTest;
import io.confluent.ksql.function.udaf.offset.EarliestByOffsetTest;
import io.confluent.ksql.function.udaf.stddev.StandardDeviationSampUdafTest;
import io.confluent.ksql.function.udaf.stddev.StandardDeviationSampleUdafTest;
import io.confluent.ksql.function.udaf.sum.BaseSumKudafTest;
import io.confluent.ksql.function.udaf.sum.DecimalSumKudafTest;
import io.confluent.ksql.function.udaf.sum.DoubleSumKudafTest;
import io.confluent.ksql.function.udaf.sum.IntegerSumKudaf;
import io.confluent.ksql.function.udaf.sum.IntegerSumKudafTest;
import io.confluent.ksql.function.udaf.sum.ListSumUdafTest;
import io.confluent.ksql.function.udaf.sum.LongSumKudafTest;
import io.confluent.ksql.function.udaf.topk.DoubleTopkKudafTest;
import io.confluent.ksql.function.udaf.topk.IntTopkKudafTest;
import io.confluent.ksql.function.udaf.topk.LongTopkKudafTest;
import io.confluent.ksql.function.udaf.topk.StringTopkKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.BytesTopKDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.DateTopKDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.DecimalTopKDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.DoubleTopkDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.IntTopkDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.LongTopkDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.StringTopkDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.TimeTopKDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.TimestampTopKDistinctKudafTest;
import io.confluent.ksql.function.udf.array.ArrayConcatTest;
import io.confluent.ksql.function.udf.array.ArrayDistinctTest;
import io.confluent.ksql.function.udf.array.ArrayExceptTest;
import io.confluent.ksql.function.udf.array.ArrayIntersectTest;
import io.confluent.ksql.function.udf.array.ArrayJoinTest;
import io.confluent.ksql.function.udf.array.ArrayLengthTest;
import io.confluent.ksql.function.udf.array.ArrayMaxTest;
import io.confluent.ksql.function.udf.array.ArrayMinTest;
import io.confluent.ksql.function.udf.array.ArrayRemoveTest;
import io.confluent.ksql.function.udf.array.ArraySortTest;
import io.confluent.ksql.function.udf.array.ArrayUnionTest;
import io.confluent.ksql.function.udf.array.EntriesTest;
import io.confluent.ksql.function.udf.array.GenerateSeriesTest;
import io.confluent.ksql.function.udf.conversions.BigIntFromBytesTest;
import io.confluent.ksql.function.udf.conversions.DoubleFromBytesTest;
import io.confluent.ksql.function.udf.conversions.IntFromBytesTest;
import io.confluent.ksql.function.udf.datetime.ConvertTzTest;
import io.confluent.ksql.function.udf.datetime.DateAddTest;
import io.confluent.ksql.function.udf.datetime.DateSubTest;
import io.confluent.ksql.function.udf.datetime.DateToStringTest;
import io.confluent.ksql.function.udf.datetime.FormatDateTest;
import io.confluent.ksql.function.udf.datetime.FormatTimeTest;
import io.confluent.ksql.function.udf.datetime.FormatTimestampTest;
import io.confluent.ksql.function.udf.datetime.FromDaysTest;
import io.confluent.ksql.function.udf.datetime.FromUnixTimeTest;
import io.confluent.ksql.function.udf.datetime.ParseDateTest;
import io.confluent.ksql.function.udf.datetime.ParseTimeTest;
import io.confluent.ksql.function.udf.datetime.ParseTimestampTest;
import io.confluent.ksql.function.udf.datetime.StringToDateTest;
import io.confluent.ksql.function.udf.datetime.StringToTimestampTest;
import io.confluent.ksql.function.udf.datetime.TimeAddTest;
import io.confluent.ksql.function.udf.datetime.TimeSubTest;
import io.confluent.ksql.function.udf.datetime.TimestampAddTest;
import io.confluent.ksql.function.udf.datetime.TimestampSubTest;
import io.confluent.ksql.function.udf.datetime.TimestampToStringTest;
import io.confluent.ksql.function.udf.datetime.UnixDateTest;
import io.confluent.ksql.function.udf.datetime.UnixTimestampTest;
import io.confluent.ksql.function.udf.geo.GeoDistanceTest;
import io.confluent.ksql.function.udf.json.IsJsonStringTest;
import io.confluent.ksql.function.udf.json.JsonArrayContainsTest;
import io.confluent.ksql.function.udf.json.JsonArrayLengthTest;
import io.confluent.ksql.function.udf.json.JsonConcatTest;
import io.confluent.ksql.function.udf.json.JsonExtractStringKudfTest;
import io.confluent.ksql.function.udf.json.JsonItemsTest;
import io.confluent.ksql.function.udf.json.JsonKeysTest;
import io.confluent.ksql.function.udf.json.JsonRecordsTest;
import io.confluent.ksql.function.udf.json.ToJsonStringTest;
import io.confluent.ksql.function.udf.lambda.FilterTest;
import io.confluent.ksql.function.udf.lambda.ReduceTest;
import io.confluent.ksql.function.udf.lambda.TransformTest;
import io.confluent.ksql.function.udf.list.ArrayContainsTest;
import io.confluent.ksql.function.udf.list.SliceTest;
import io.confluent.ksql.function.udf.map.AsMapTest;
import io.confluent.ksql.function.udf.map.MapKeysTest;
import io.confluent.ksql.function.udf.map.MapUnionTest;
import io.confluent.ksql.function.udf.map.MapValuesTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        AggregateAnalyzerTest.class,
        AnalysisTest.class,
        AnalyzerFunctionalTest.class,
        ColumnReferenceValidatorTest.class,
        FilterTypeValidatorTest.class,
        PullQueryValidatorTest.class,
        PushQueryValidatorTest.class,
        QueryAnalyzerFunctionalTest.class,
        QueryAnalyzerTest.class,
        QueryValidatorUtilTest.class,
        RewrittenAnalysisTest.class,
        SourceSchemasTest.class,
        JdbcSourceTest.class,
        ConnectorTest.class,
        AlterSourceFactoryTest.class,
        CommandFactoriesTest.class,
        CreateSourceFactoryTest.class,
        DdlCommandExecTest.class,
        DropSourceFactoryTest.class,
        DropTypeFactoryTest.class,
        RegisterTypeFactoryTest.class,
        KsqlContextTest.class,
        GenericExpressionResolverTest.class,
        GenericRecordFactoryTest.class,
        AstSanitizerTest.class,
        DataSourceExtractorTest.class,
        ExpressionTreeRewriterTest.class,
//        QueryAnonymizerTest.class, todo this test is causing problems.
        StatementRewriteForMagicPseudoTimestampTest.class,
        StatementRewriterTest.class,
        ImmutabilityTest.class,
        KsqlEngineTest.class,
        KsqlPlanV1Test.class,
        OrphanedTransientQueryCleanerTest.class,
        QueryIdUtilTest.class,
        QueryPlanTest.class,
        RuntimeAssignorTest.class,
        TransientQueryCleanupServiceTest.class,
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
        DefaultFormatInjectorTest.class,
        CollectListUdafTest.class,
        CollectSetUdafTest.class,
        AttrTest.class,
        AverageUdafTest.class,
        CorrelationUdafTest.class,
        CountDistinctKudafTest.class,
        CountKudafTest.class,
        HistogramUdafTest.class,
        BytesMaxKudafTest.class,
        DateMaxKudafTest.class,
        DecimalMaxKudafTest.class,
        DoubleMaxKudafTest.class,
        IntegerMaxKudafTest.class,
        LongMaxKudafTest.class,
        StringMaxKudafTest.class,
        TimeMaxKudafTest.class,
        TimestampMaxKudafTest.class,
        BytesMinKudafTest.class,
        DateMinKudafTest.class,
        DecimalMinKudafTest.class,
        DoubleMinKudafTest.class,
        IntegerMinKudafTest.class,
        LongMinKudafTest.class,
        StringMinKudafTest.class,
        TimeMinKudafTest.class,
        TimestampMinKudafTest.class,
        EarliestByOffsetTest.class,
        StandardDeviationSampleUdafTest.class,
        StandardDeviationSampUdafTest.class,
        BaseSumKudafTest.class,
        DecimalSumKudafTest.class,
        DoubleSumKudafTest.class,
        DoubleSumKudafTest.class,
        IntegerSumKudafTest.class,
        ListSumUdafTest.class,
        LongSumKudafTest.class,
        DoubleTopkKudafTest.class,
        IntTopkKudafTest.class,
        LongTopkKudafTest.class,
        StringTopkKudafTest.class,
        BytesTopKDistinctKudafTest.class,
        DateTopKDistinctKudafTest.class,
        DecimalTopKDistinctKudafTest.class,
        DoubleTopkDistinctKudafTest.class,
        IntTopkDistinctKudafTest.class,
        LongTopkDistinctKudafTest.class,
        StringTopkDistinctKudafTest.class,
        TimestampTopKDistinctKudafTest.class,
        TimeTopKDistinctKudafTest.class,
        ArrayConcatTest.class,
        ArrayDistinctTest.class,
        ArrayExceptTest.class,
        ArrayIntersectTest.class,
        ArrayJoinTest.class,
        ArrayLengthTest.class,
        ArrayMaxTest.class,
        ArrayMinTest.class,
        ArrayRemoveTest.class,
        ArraySortTest.class,
        ArrayUnionTest.class,
        EntriesTest.class,
        GenerateSeriesTest.class,
        BigIntFromBytesTest.class,
        DoubleFromBytesTest.class,
        IntFromBytesTest.class,
        ConvertTzTest.class,
        DateAddTest.class,
        DateSubTest.class,
        DateToStringTest.class,
        FormatDateTest.class,
        FormatTimestampTest.class,
        FormatTimeTest.class,
        FromDaysTest.class,
        FromUnixTimeTest.class,
        ParseDateTest.class,
        ParseTimestampTest.class,
        ParseTimeTest.class,
        StringToDateTest.class,
        StringToTimestampTest.class,
        TimeAddTest.class,
        TimestampAddTest.class,
        TimestampSubTest.class,
        TimestampToStringTest.class,
        TimeSubTest.class,
        UnixDateTest.class,
        UnixTimestampTest.class,
        GeoDistanceTest.class,
        IsJsonStringTest.class,
        JsonArrayContainsTest.class,
        JsonArrayLengthTest.class,
        JsonConcatTest.class,
        JsonExtractStringKudfTest.class,
        JsonItemsTest.class,
        JsonKeysTest.class,
        JsonRecordsTest.class,
        ToJsonStringTest.class,
        FilterTest.class,
        ReduceTest.class,
        TransformTest.class,
        ArrayContainsTest.class,
        SliceTest.class,
        AsMapTest.class,
        MapKeysTest.class,
        MapValuesTest.class,
        MapUnionTest.class,
        // todo I didn't add all the tests here.
})
public class MyTestsEngine {
}
