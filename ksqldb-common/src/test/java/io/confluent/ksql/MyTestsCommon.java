package io.confluent.ksql;

import io.confluent.ksql.common.ImmutabilityTest;
import io.confluent.ksql.config.ConfigItemTest;
import io.confluent.ksql.config.KsqlConfigResolverTest;
import io.confluent.ksql.config.SessionConfigTest;
import io.confluent.ksql.configdef.ConfigValidatorsTest;
import io.confluent.ksql.errors.ProductionExceptionHandlerUtilTest;
import io.confluent.ksql.function.ParameterInfoTest;
import io.confluent.ksql.function.UdfFactoryTest;
import io.confluent.ksql.function.UdfIndexTest;
import io.confluent.ksql.function.types.ParamTypesTest;
import io.confluent.ksql.function.udf.UdfMetadataTest;
import io.confluent.ksql.json.LogicalSchemaSerializerTest;
import io.confluent.ksql.json.StructSerializationModuleTest;
import io.confluent.ksql.logging.processing.MeteredProcessingLoggerFactoryTest;
import io.confluent.ksql.logging.processing.MeteredProcessingLoggerTest;
import io.confluent.ksql.logging.processing.ProcessingLoggerImplTest;
import io.confluent.ksql.logging.processing.ProcessingLoggerUtilTest;
import io.confluent.ksql.metrics.ConsumerCollectorTest;
import io.confluent.ksql.metrics.MetricCollectorsTest;
import io.confluent.ksql.metrics.ProducerCollectorTest;
import io.confluent.ksql.metrics.StreamsErrorCollectorTest;
import io.confluent.ksql.metrics.TopicSensorsTest;
import io.confluent.ksql.model.WindowTypeTest;
import io.confluent.ksql.parser.DurationParserTest;
import io.confluent.ksql.properties.DenyListPropertyValidatorTest;
import io.confluent.ksql.properties.LocalPropertiesTest;
import io.confluent.ksql.properties.LocalPropertyParserTest;
import io.confluent.ksql.properties.PropertiesUtilTest;
import io.confluent.ksql.query.QueryErrorClassifierTest;
import io.confluent.ksql.query.QueryIdTest;
import io.confluent.ksql.reactive.BufferedPublisherTest;
import io.confluent.ksql.reactive.PublisherTestBase;
import io.confluent.ksql.reactive.ReactiveSubscriberTest;
import io.confluent.ksql.schema.OperatorTest;
import io.confluent.ksql.schema.connect.SchemaWalkerTest;
import io.confluent.ksql.schema.connect.SqlSchemaFormatterTest;
import io.confluent.ksql.schema.ksql.ColumnTest;
import io.confluent.ksql.schema.ksql.LogicalSchemaTest;
import io.confluent.ksql.schema.ksql.PersistenceSchemaTest;
import io.confluent.ksql.schema.ksql.SchemaConvertersTest;
import io.confluent.ksql.schema.ksql.SqlBaseTypeTest;
import io.confluent.ksql.schema.ksql.SqlTypeWalkerTest;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.serde.FormatInfoTest;
import io.confluent.ksql.serde.RefinementInfoTest;
import io.confluent.ksql.serde.SerdeFeaturesTest;
import io.confluent.ksql.serde.WindowInfoTest;
import io.confluent.ksql.util.BytesUtilTest;
import io.confluent.ksql.util.CompatibleSetTest;
import io.confluent.ksql.util.ConsistencyOffsetVectorTest;
import io.confluent.ksql.util.DecimalUtilTest;
import io.confluent.ksql.util.ErrorMessageUtilTest;
import io.confluent.ksql.util.GenericsUtilTest;
import io.confluent.ksql.util.GrammaticalJoinerTest;
import io.confluent.ksql.util.HandlerMapsTest;
import io.confluent.ksql.util.KsqlConfigTest;
import io.confluent.ksql.util.KsqlRequestConfigTest;
import io.confluent.ksql.util.KsqlVersionTest;
import io.confluent.ksql.util.PushOffsetRangeTest;
import io.confluent.ksql.util.PushOffsetVectorTest;
import io.confluent.ksql.util.ReservedInternalTopicsTest;
import io.confluent.ksql.util.RetryUtilTest;
import io.confluent.ksql.util.VertxSslOptionsFactoryTest;
import io.confluent.ksql.util.WelcomeMsgUtilsTest;
import io.confluent.ksql.util.timestamp.PartialStringToTimestampParserTest;
import io.confluent.ksql.util.timestamp.StringToTimestampParserTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ImmutabilityTest.class,
        ConfigItemTest.class,
        KsqlConfigResolverTest.class,
        SessionConfigTest.class,
        ConfigValidatorsTest.class,
        ProductionExceptionHandlerUtilTest.class,
        ParamTypesTest.class,
        UdfMetadataTest.class,
        ParameterInfoTest.class,
        UdfFactoryTest.class,
        UdfIndexTest.class,
        LogicalSchemaSerializerTest.class,
        StructSerializationModuleTest.class,
        MeteredProcessingLoggerFactoryTest.class,
        MeteredProcessingLoggerTest.class,
        ProcessingLoggerImplTest.class,
        ProcessingLoggerUtilTest.class,
        ConsumerCollectorTest.class,
        MetricCollectorsTest.class,
        ProducerCollectorTest.class,
        StreamsErrorCollectorTest.class,
        TopicSensorsTest.class,
        WindowTypeTest.class,
        DurationParserTest.class,
        DenyListPropertyValidatorTest.class,
        LocalPropertiesTest.class,
        LocalPropertyParserTest.class,
        PropertiesUtilTest.class,
        QueryErrorClassifierTest.class,
        QueryIdTest.class,
        BufferedPublisherTest.class,
        ReactiveSubscriberTest.class,
        SchemaWalkerTest.class,
        SqlSchemaFormatterTest.class,
        ColumnTest.class,
        LogicalSchemaTest.class,
        PersistenceSchemaTest.class,
        SchemaConvertersTest.class,
        SqlBaseTypeTest.class,
        SqlTypeWalkerTest.class,
        OperatorTest.class,
        FormatInfoTest.class,
        RefinementInfoTest.class,
        SerdeFeaturesTest.class,
        WindowInfoTest.class,
        PartialStringToTimestampParserTest.class,
        StringToTimestampParserTest.class,
        BytesUtilTest.class,
        CompatibleSetTest.class,
        ConsistencyOffsetVectorTest.class,
        DecimalUtilTest.class,
        ErrorMessageUtilTest.class,
        GenericsUtilTest.class,
        GrammaticalJoinerTest.class,
        HandlerMapsTest.class,
        KsqlConfigTest.class,
        KsqlRequestConfigTest.class,
        KsqlVersionTest.class,
        PushOffsetRangeTest.class,
        PushOffsetVectorTest.class,
        ReservedInternalTopicsTest.class,
        RetryUtilTest.class,
        VertxSslOptionsFactoryTest.class,
        WelcomeMsgUtilsTest.class,
        GenericKeyTest.class,
        GenericRowTest.class,
        QueryApplicationIdTest.class,
})
public class MyTestsCommon{
}