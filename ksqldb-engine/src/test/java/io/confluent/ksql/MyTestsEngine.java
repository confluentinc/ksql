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
import io.confluent.ksql.function.BaseAggregateFunctionTest;
import io.confluent.ksql.function.BlacklistTest;
import io.confluent.ksql.function.FunctionMetricsTest;
import io.confluent.ksql.function.InternalFunctionRegistryTest;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.function.UdafAggregateFunctionFactoryTest;
import io.confluent.ksql.function.UdafTypesTest;
import io.confluent.ksql.function.UdfClassLoaderTest;
import io.confluent.ksql.function.UdfLoaderTest;
import io.confluent.ksql.function.UdfMetricProducerTest;
import io.confluent.ksql.function.UdtfLoaderTest;
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
import io.confluent.ksql.function.udf.AsValueTest;
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
import io.confluent.ksql.function.udf.math.AbsTest;
import io.confluent.ksql.function.udf.math.AcosTest;
import io.confluent.ksql.function.udf.math.AsinTest;
import io.confluent.ksql.function.udf.math.Atan2Test;
import io.confluent.ksql.function.udf.math.AtanTest;
import io.confluent.ksql.function.udf.math.CbrtTest;
import io.confluent.ksql.function.udf.math.CosTest;
import io.confluent.ksql.function.udf.math.CoshTest;
import io.confluent.ksql.function.udf.math.CotTest;
import io.confluent.ksql.function.udf.math.DegreesTest;
import io.confluent.ksql.function.udf.math.ExpTest;
import io.confluent.ksql.function.udf.math.GreatestTest;
import io.confluent.ksql.function.udf.math.LeastTest;
import io.confluent.ksql.function.udf.math.LnTest;
import io.confluent.ksql.function.udf.math.LogTest;
import io.confluent.ksql.function.udf.math.PiTest;
import io.confluent.ksql.function.udf.math.PowerTest;
import io.confluent.ksql.function.udf.math.RadiansTest;
import io.confluent.ksql.function.udf.math.RandomTest;
import io.confluent.ksql.function.udf.math.RoundTest;
import io.confluent.ksql.function.udf.math.SignTest;
import io.confluent.ksql.function.udf.math.SinTest;
import io.confluent.ksql.function.udf.math.SinhTest;
import io.confluent.ksql.function.udf.math.SqrtTest;
import io.confluent.ksql.function.udf.math.TanTest;
import io.confluent.ksql.function.udf.math.TanhTest;
import io.confluent.ksql.function.udf.math.TruncTest;
import io.confluent.ksql.function.udf.nulls.CoalesceTest;
import io.confluent.ksql.function.udf.nulls.NullIfTest;
import io.confluent.ksql.function.udf.string.ChrTest;
import io.confluent.ksql.function.udf.string.ConcatTest;
import io.confluent.ksql.function.udf.string.ConcatWSTest;
import io.confluent.ksql.function.udf.string.EltTest;
import io.confluent.ksql.function.udf.string.EncodeTest;
import io.confluent.ksql.function.udf.string.FieldTest;
import io.confluent.ksql.function.udf.string.FromBytesTest;
import io.confluent.ksql.function.udf.string.InitCapTest;
import io.confluent.ksql.function.udf.string.InstrTest;
import io.confluent.ksql.function.udf.string.LCaseTest;
import io.confluent.ksql.function.udf.string.LPadTest;
import io.confluent.ksql.function.udf.string.LenTest;
import io.confluent.ksql.function.udf.string.MaskKeepLeftTest;
import io.confluent.ksql.function.udf.string.MaskKeepRightTest;
import io.confluent.ksql.function.udf.string.MaskLeftTest;
import io.confluent.ksql.function.udf.string.MaskRightTest;
import io.confluent.ksql.function.udf.string.MaskTest;
import io.confluent.ksql.function.udf.string.MaskerTest;
import io.confluent.ksql.function.udf.string.RPadTest;
import io.confluent.ksql.function.udf.string.RegexpExtractAllTest;
import io.confluent.ksql.function.udf.string.RegexpExtractTest;
import io.confluent.ksql.function.udf.string.RegexpReplaceTest;
import io.confluent.ksql.function.udf.string.RegexpSplitToArrayTest;
import io.confluent.ksql.function.udf.string.ReplaceTest;
import io.confluent.ksql.function.udf.string.SplitTest;
import io.confluent.ksql.function.udf.string.SplitToMapTest;
import io.confluent.ksql.function.udf.string.SubstringTest;
import io.confluent.ksql.function.udf.string.ToBytesTest;
import io.confluent.ksql.function.udf.string.TrimTest;
import io.confluent.ksql.function.udf.string.UCaseTest;
import io.confluent.ksql.function.udf.string.UuidTest;
import io.confluent.ksql.function.udf.url.UrlDecodeParamTest;
import io.confluent.ksql.function.udf.url.UrlEncodeParamTest;
import io.confluent.ksql.function.udf.url.UrlExtractFragmentTest;
import io.confluent.ksql.function.udf.url.UrlExtractHostTest;
import io.confluent.ksql.function.udf.url.UrlExtractParameterTest;
import io.confluent.ksql.function.udf.url.UrlExtractPathTest;
import io.confluent.ksql.function.udf.url.UrlExtractPortTest;
import io.confluent.ksql.function.udf.url.UrlExtractProtocolTest;
import io.confluent.ksql.function.udf.url.UrlExtractQueryTest;
import io.confluent.ksql.function.udtf.CubeTest;
import io.confluent.ksql.integration.DependentStatementsIntegrationTest;
import io.confluent.ksql.integration.EndToEndIntegrationTest;
import io.confluent.ksql.integration.JoinIntTest;
import io.confluent.ksql.integration.JsonFormatTest;
import io.confluent.ksql.integration.KafkaConsumerGroupClientTest;
import io.confluent.ksql.integration.ReplaceIntTest;
import io.confluent.ksql.integration.ReplaceWithSharedRuntimesIntTest;
import io.confluent.ksql.integration.SecureIntegrationTest;
import io.confluent.ksql.integration.SelectValueMapperIntegrationTest;
import io.confluent.ksql.integration.StreamsSelectAndProjectIntTest;
import io.confluent.ksql.integration.UdfIntTest;
import io.confluent.ksql.integration.WindowingIntTest;
import io.confluent.ksql.integration.WindowingSharedRuntimeIntTest;
import io.confluent.ksql.internal.JmxDataPointsReporterTest;
import io.confluent.ksql.internal.KsqlEngineMetricsTest;
import io.confluent.ksql.internal.QueryStateMetricsReportingListenerTest;
import io.confluent.ksql.internal.StorageUtilizationMetricsReporterTest;
import io.confluent.ksql.internal.ThroughputMetricsReporterTest;
import io.confluent.ksql.logging.query.QueryLoggerMessageTest;
import io.confluent.ksql.logging.query.QueryLoggerTest;
import io.confluent.ksql.materialization.ks.KsMaterializationFunctionalTest;
import io.confluent.ksql.planner.JoinTreeTest;
import io.confluent.ksql.planner.LogicalPlannerTest;
import io.confluent.ksql.planner.ProjectionTest;
import io.confluent.ksql.planner.RequiredColumnsTest;
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
import io.confluent.ksql.query.AuthorizationClassifierTest;
import io.confluent.ksql.query.KafkaStreamsQueryValidatorTest;
import io.confluent.ksql.query.KsqlFunctionClassifierTest;
import io.confluent.ksql.query.KsqlSerializationClassifierTest;
import io.confluent.ksql.query.MissingSubjectClassifierTest;
import io.confluent.ksql.query.MissingTopicClassifierTest;
import io.confluent.ksql.query.PullQueryWriteStreamTest;
import io.confluent.ksql.query.QueryBuilderTest;
import io.confluent.ksql.query.QueryRegistryImplTest;
import io.confluent.ksql.query.RegexClassifierTest;
import io.confluent.ksql.query.SchemaAuthorizationClassifierTest;
import io.confluent.ksql.query.TransientQueryQueueTest;
import io.confluent.ksql.query.id.SequentialQueryIdGeneratorTest;
import io.confluent.ksql.query.id.SpecificQueryIdGeneratorTest;
import io.confluent.ksql.schema.ksql.inference.DefaultSchemaInjectorFunctionalTest;
import io.confluent.ksql.schema.ksql.inference.DefaultSchemaInjectorTest;
import io.confluent.ksql.schema.ksql.inference.SchemaRegisterInjectorTest;
import io.confluent.ksql.schema.ksql.inference.SchemaRegistryTopicSchemaSupplierTest;
import io.confluent.ksql.schema.registry.KsqlSchemaRegistryClientFactoryTest;
import io.confluent.ksql.schema.registry.SchemaRegistryUtilTest;
import io.confluent.ksql.security.DefaultKsqlPrincipalTest;
import io.confluent.ksql.security.ExtensionSecurityManagerTest;
import io.confluent.ksql.security.KsqlAuthorizationValidatorFactoryTest;
import io.confluent.ksql.security.KsqlAuthorizationValidatorImplTest;
import io.confluent.ksql.security.KsqlBackendAccessValidatorTest;
import io.confluent.ksql.security.KsqlCacheAccessValidatorTest;
import io.confluent.ksql.security.KsqlProvidedAccessValidatorTest;
import io.confluent.ksql.serde.SerdeFeaturesFactoryTest;
import io.confluent.ksql.services.DefaultConnectClientFactoryTest;
import io.confluent.ksql.services.DefaultConnectClientTest;
import io.confluent.ksql.services.KafkaTopicClientImplIntegrationTest;
import io.confluent.ksql.services.KafkaTopicClientImplTest;
import io.confluent.ksql.services.MemoizedSupplierTest;
import io.confluent.ksql.services.SandboxConnectClientTest;
import io.confluent.ksql.services.SandboxedAdminClientTest;
import io.confluent.ksql.services.SandboxedConsumerTest;
import io.confluent.ksql.services.SandboxedKafkaClientSupplierTest;
import io.confluent.ksql.services.SandboxedKafkaTopicClientTest;
import io.confluent.ksql.services.SandboxedProducerTest;
import io.confluent.ksql.services.SandboxedSchemaRegistryClientTest;
import io.confluent.ksql.services.SandboxedServiceContextTest;
import io.confluent.ksql.services.TestServiceContextTest;
import io.confluent.ksql.statement.ConfiguredStatementTest;
import io.confluent.ksql.statement.InjectorChainTest;
import io.confluent.ksql.statement.SourcePropertyInjectorTest;
import io.confluent.ksql.streams.GroupedFactoryTest;
import io.confluent.ksql.streams.JoinedFactoryTest;
import io.confluent.ksql.structured.QueryContextTest;
import io.confluent.ksql.structured.SchemaKGroupedStreamTest;
import io.confluent.ksql.structured.SchemaKGroupedTableTest;
import io.confluent.ksql.structured.SchemaKSourceFactoryTest;
import io.confluent.ksql.structured.SchemaKStreamTest;
import io.confluent.ksql.structured.SchemaKTableTest;
import io.confluent.ksql.topic.SourceTopicsExtractorTest;
import io.confluent.ksql.topic.TopicCreateInjectorTest;
import io.confluent.ksql.topic.TopicDeleteInjectorTest;
import io.confluent.ksql.topic.TopicPropertiesTest;
import io.confluent.ksql.util.ArrayUtilTest;
import io.confluent.ksql.util.BinPackedPersistentQueryMetadataImplTest;
import io.confluent.ksql.util.ExecutorUtilTest;
import io.confluent.ksql.util.FileWatcherTest;
import io.confluent.ksql.util.KafkaStreamsThreadErrorTest;
import io.confluent.ksql.util.LimitedProxyBuilderTest;
import io.confluent.ksql.util.PersistentQueryMetadataTest;
import io.confluent.ksql.util.PlanSummaryTest;
import io.confluent.ksql.util.QueryLoggerUtilTest;
import io.confluent.ksql.util.QueryMetadataTest;
import io.confluent.ksql.util.SandboxedPersistentQueryMetadataImplTest;
import io.confluent.ksql.util.SandboxedSharedKafkaStreamsRuntimeImplTest;
import io.confluent.ksql.util.SandboxedTransientQueryMetadataTest;
import io.confluent.ksql.util.ScalablePushQueryMetadataTest;
import io.confluent.ksql.util.SharedKafkaStreamsRuntimeImplTest;
import io.confluent.ksql.util.TransientQueryMetadataTest;
import io.confluent.ksql.util.json.JsonPathTokenizerTest;
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
        MapUnionTest.class,
        MapValuesTest.class,
        AbsTest.class,
        AcosTest.class,
        AsinTest.class,
        Atan2Test.class,
        AtanTest.class,
        CbrtTest.class,
        CoshTest.class,
        CosTest.class,
        CotTest.class,
        DegreesTest.class,
        ExpTest.class,
        GreatestTest.class,
        LeastTest.class,
        LnTest.class,
        LogTest.class,
        PiTest.class,
        PowerTest.class,
        RadiansTest.class,
        RandomTest.class,
        RoundTest.class,
        SignTest.class,
        SinhTest.class,
        SinTest.class,
        SqrtTest.class,
        TanhTest.class,
        TanTest.class,
        TruncTest.class,
        CoalesceTest.class,
        NullIfTest.class,
        ChrTest.class,
        ConcatTest.class,
        ConcatWSTest.class,
        EltTest.class,
        EncodeTest.class,
        FieldTest.class,
        FromBytesTest.class,
        InitCapTest.class,
        InstrTest.class,
        LCaseTest.class,
        LenTest.class,
        LPadTest.class,
        MaskerTest.class,
        MaskKeepLeftTest.class,
        MaskKeepRightTest.class,
        MaskLeftTest.class,
        MaskRightTest.class,
        MaskTest.class,
        RegexpExtractAllTest.class,
        RegexpExtractTest.class,
        RegexpReplaceTest.class,
        RegexpSplitToArrayTest.class,
        ReplaceTest.class,
        RPadTest.class,
        SplitTest.class,
        SplitToMapTest.class,
        SubstringTest.class,
        ToBytesTest.class,
        TrimTest.class,
        UCaseTest.class,
        UuidTest.class,
        UrlDecodeParamTest.class,
        UrlEncodeParamTest.class,
        UrlExtractFragmentTest.class,
        UrlExtractHostTest.class,
        UrlExtractParameterTest.class,
        UrlExtractPathTest.class,
        UrlExtractPortTest.class,
        UrlExtractProtocolTest.class,
        UrlExtractQueryTest.class,
        AsValueTest.class,
        CubeTest.class,
        BaseAggregateFunctionTest.class,
        BlacklistTest.class,
        FunctionMetricsTest.class,
        InternalFunctionRegistryTest.class,
        TestFunctionRegistry.class,
        UdafAggregateFunctionFactoryTest.class,
        UdafTypesTest.class,
        UdfClassLoaderTest.class,
//        UdfLoaderTest.class,
        UdfMetricProducerTest.class,
//        UdtfLoaderTest.class,
        DependentStatementsIntegrationTest.class,
        EndToEndIntegrationTest.class,
        JoinIntTest.class,
//        JsonFormatTest.class,
        KafkaConsumerGroupClientTest.class,
        ReplaceIntTest.class,
        ReplaceWithSharedRuntimesIntTest.class,
        SecureIntegrationTest.class,
        SelectValueMapperIntegrationTest.class,
        StreamsSelectAndProjectIntTest.class,
        UdfIntTest.class,
        WindowingIntTest.class,
        WindowingSharedRuntimeIntTest.class,
        JmxDataPointsReporterTest.class,
        KsqlEngineMetricsTest.class,
        QueryStateMetricsReportingListenerTest.class,
        StorageUtilizationMetricsReporterTest.class,
        ThroughputMetricsReporterTest.class,
        QueryLoggerMessageTest.class,
        QueryLoggerTest.class,
        KsMaterializationFunctionalTest.class,
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
        SequentialQueryIdGeneratorTest.class,
        SpecificQueryIdGeneratorTest.class,
        AuthorizationClassifierTest.class,
        KafkaStreamsQueryValidatorTest.class,
        KsqlFunctionClassifierTest.class,
        KsqlSerializationClassifierTest.class,
        MissingSubjectClassifierTest.class,
        MissingTopicClassifierTest.class,
        PullQueryWriteStreamTest.class,
        QueryBuilderTest.class,
        QueryRegistryImplTest.class,
        RegexClassifierTest.class,
        SchemaAuthorizationClassifierTest.class,
        TransientQueryQueueTest.class,
        DefaultSchemaInjectorFunctionalTest.class,
        DefaultSchemaInjectorTest.class,
        SchemaRegisterInjectorTest.class,
        SchemaRegistryTopicSchemaSupplierTest.class,
        KsqlSchemaRegistryClientFactoryTest.class,
        SchemaRegistryUtilTest.class,
        DefaultKsqlPrincipalTest.class,
        ExtensionSecurityManagerTest.class,
        KsqlAuthorizationValidatorFactoryTest.class,
        KsqlAuthorizationValidatorImplTest.class,
        KsqlBackendAccessValidatorTest.class,
        KsqlCacheAccessValidatorTest.class,
        KsqlProvidedAccessValidatorTest.class,
        SerdeFeaturesFactoryTest.class,
        DefaultConnectClientFactoryTest.class,
        DefaultConnectClientTest.class,
        KafkaTopicClientImplIntegrationTest.class,
        KafkaTopicClientImplTest.class,
        MemoizedSupplierTest.class,
        SandboxConnectClientTest.class,
        SandboxedAdminClientTest.class,
        SandboxedConsumerTest.class,
        SandboxedKafkaClientSupplierTest.class,
        SandboxedKafkaTopicClientTest.class,
        SandboxedProducerTest.class,
        SandboxedSchemaRegistryClientTest.class,
        SandboxedServiceContextTest.class,
        TestServiceContextTest.class,
        ConfiguredStatementTest.class,
        InjectorChainTest.class,
        SourcePropertyInjectorTest.class,
        GroupedFactoryTest.class,
        JoinedFactoryTest.class,
        QueryContextTest.class,
        SchemaKGroupedStreamTest.class,
        SchemaKGroupedTableTest.class,
        SchemaKSourceFactoryTest.class,
        SchemaKStreamTest.class,
//        SchemaKTableTest.class,
        SourceTopicsExtractorTest.class,
        TopicCreateInjectorTest.class,
        TopicDeleteInjectorTest.class,
        TopicPropertiesTest.class,
        JsonPathTokenizerTest.class,
        ArrayUtilTest.class,
        BinPackedPersistentQueryMetadataImplTest.class,
        ExecutorUtilTest.class,
        FileWatcherTest.class,
        KafkaStreamsThreadErrorTest.class,
        LimitedProxyBuilderTest.class,
        PersistentQueryMetadataTest.class,
        PlanSummaryTest.class,
        QueryLoggerUtilTest.class,
        QueryMetadataTest.class,
        SandboxedPersistentQueryMetadataImplTest.class,
        SandboxedSharedKafkaStreamsRuntimeImplTest.class,
        SandboxedTransientQueryMetadataTest.class,
        ScalablePushQueryMetadataTest.class,
        SharedKafkaStreamsRuntimeImplTest.class,
        TransientQueryMetadataTest.class,
        KsqlContextTestUtilTest.class,
})
public class MyTestsEngine {
}
