package io.confluent.ksql;

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
import io.confluent.ksql.format.DefaultFormatInjectorTest;
import io.confluent.ksql.internal.JmxDataPointsReporterTest;
import io.confluent.ksql.internal.KsqlEngineMetricsTest;
import io.confluent.ksql.internal.QueryStateMetricsReportingListenerTest;
import io.confluent.ksql.internal.StorageUtilizationMetricsReporterTest;
import io.confluent.ksql.internal.ThroughputMetricsReporterTest;
import io.confluent.ksql.logging.query.QueryLoggerMessageTest;
import io.confluent.ksql.logging.query.QueryLoggerTest;
import io.confluent.ksql.materialization.ks.KsMaterializationFunctionalTest;
import io.confluent.ksql.schema.ksql.inference.DefaultSchemaInjectorFunctionalTest;
import io.confluent.ksql.schema.ksql.inference.DefaultSchemaInjectorTest;
import io.confluent.ksql.schema.ksql.inference.SchemaRegisterInjectorTest;
import io.confluent.ksql.schema.ksql.inference.SchemaRegistryTopicSchemaSupplierTest;
import io.confluent.ksql.schema.registry.KsqlSchemaRegistryClientFactoryTest;
import io.confluent.ksql.schema.registry.SchemaRegistryUtilTest;
import io.confluent.ksql.serde.SerdeFeaturesFactoryTest;
import io.confluent.ksql.statement.ConfiguredStatementTest;
import io.confluent.ksql.statement.InjectorChainTest;
import io.confluent.ksql.statement.SourcePropertyInjectorTest;
import io.confluent.ksql.streams.GroupedFactoryTest;
import io.confluent.ksql.streams.JoinedFactoryTest;
import io.confluent.ksql.topic.SourceTopicsExtractorTest;
import io.confluent.ksql.topic.TopicCreateInjectorTest;
import io.confluent.ksql.topic.TopicDeleteInjectorTest;
import io.confluent.ksql.topic.TopicPropertiesTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
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
        DefaultFormatInjectorTest.class,
        JmxDataPointsReporterTest.class,
        KsqlEngineMetricsTest.class,
        QueryStateMetricsReportingListenerTest.class,
        StorageUtilizationMetricsReporterTest.class,
        ThroughputMetricsReporterTest.class,
        QueryLoggerMessageTest.class,
        QueryLoggerTest.class,
        KsMaterializationFunctionalTest.class,
        DefaultSchemaInjectorFunctionalTest.class,
        DefaultSchemaInjectorTest.class,
        SchemaRegisterInjectorTest.class,
        SchemaRegistryTopicSchemaSupplierTest.class,
        KsqlSchemaRegistryClientFactoryTest.class,
        SchemaRegistryUtilTest.class,
        SerdeFeaturesFactoryTest.class,
        ConfiguredStatementTest.class,
        InjectorChainTest.class,
        SourcePropertyInjectorTest.class,
        GroupedFactoryTest.class,
        JoinedFactoryTest.class,
        SourceTopicsExtractorTest.class,
        TopicCreateInjectorTest.class,
        TopicDeleteInjectorTest.class,
        TopicPropertiesTest.class,
})
public class MyTestsEngine {
}
