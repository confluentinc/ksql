package io.confluent.ksql;

import io.confluent.ksql.parser.AssertTableTest;
import io.confluent.ksql.parser.AstBuilderTest;
import io.confluent.ksql.parser.ColumnReferenceParserTest;
import io.confluent.ksql.parser.ExpressionParserTest;
import io.confluent.ksql.parser.ImmutabilityTest;
import io.confluent.ksql.parser.KsqlParserTest;
import io.confluent.ksql.parser.LiteralUtilTest;
import io.confluent.ksql.parser.ReservedWordMetaTest;
import io.confluent.ksql.parser.SchemaParserTest;
import io.confluent.ksql.parser.SqlFormatterTest;
import io.confluent.ksql.parser.SyntaxErrorValidatorTest;
import io.confluent.ksql.parser.VariableSubstitutorTest;
import io.confluent.ksql.parser.json.ColumnSerdeTest;
import io.confluent.ksql.parser.json.ExpressionDeserializerTest;
import io.confluent.ksql.parser.json.ExpressionSerializerTest;
import io.confluent.ksql.parser.json.KsqlTypesSerdeModuleTest;
import io.confluent.ksql.parser.json.LogicalSchemaDeserializerTest;
import io.confluent.ksql.parser.json.SelectExpressionDeserializerTest;
import io.confluent.ksql.parser.json.SelectExpressionSerializerTest;
import io.confluent.ksql.parser.json.WindowExpressionDeserializerTest;
import io.confluent.ksql.parser.json.WindowExpressionSerializerTest;
import io.confluent.ksql.parser.properties.with.CreateSourceAsPropertiesTest;
import io.confluent.ksql.parser.properties.with.CreateSourcePropertiesTest;
import io.confluent.ksql.parser.properties.with.InsertIntoPropertiesTest;
import io.confluent.ksql.parser.tree.AliasedRelationTest;
import io.confluent.ksql.parser.tree.AllColumnsTest;
import io.confluent.ksql.parser.tree.AssertSchemaTest;
import io.confluent.ksql.parser.tree.AssertStreamTest;
import io.confluent.ksql.parser.tree.AssertTopicTest;
import io.confluent.ksql.parser.tree.AssertValuesTest;
import io.confluent.ksql.parser.tree.CreateConnectorTest;
import io.confluent.ksql.parser.tree.CreateStreamAsSelectTest;
import io.confluent.ksql.parser.tree.CreateStreamTest;
import io.confluent.ksql.parser.tree.CreateTableAsSelectTest;
import io.confluent.ksql.parser.tree.CreateTableTest;
import io.confluent.ksql.parser.tree.DescribeConnectorTest;
import io.confluent.ksql.parser.tree.DropStreamTest;
import io.confluent.ksql.parser.tree.DropTableTest;
import io.confluent.ksql.parser.tree.ExplainTest;
import io.confluent.ksql.parser.tree.GroupByTest;
import io.confluent.ksql.parser.tree.HoppingWindowExpressionTest;
import io.confluent.ksql.parser.tree.InsertIntoTest;
import io.confluent.ksql.parser.tree.InsertValuesTest;
import io.confluent.ksql.parser.tree.JoinTest;
import io.confluent.ksql.parser.tree.JoinedSourceTest;
import io.confluent.ksql.parser.tree.ListConnectorPluginsTest;
import io.confluent.ksql.parser.tree.ListConnectorsTest;
import io.confluent.ksql.parser.tree.ListPropertiesTest;
import io.confluent.ksql.parser.tree.ListQueriesTest;
import io.confluent.ksql.parser.tree.ListStreamsTest;
import io.confluent.ksql.parser.tree.ListTablesTest;
import io.confluent.ksql.parser.tree.ListTopicsTest;
import io.confluent.ksql.parser.tree.ParserModelTest;
import io.confluent.ksql.parser.tree.PartitionByTest;
import io.confluent.ksql.parser.tree.QueryTest;
import io.confluent.ksql.parser.tree.SessionWindowExpressionTest;
import io.confluent.ksql.parser.tree.ShowColumnsTest;
import io.confluent.ksql.parser.tree.SingleColumnTest;
import io.confluent.ksql.parser.tree.StatementsTest;
import io.confluent.ksql.parser.tree.StructAllTest;
import io.confluent.ksql.parser.tree.TableElementTest;
import io.confluent.ksql.parser.tree.TableElementsTest;
import io.confluent.ksql.parser.tree.TableTest;
import io.confluent.ksql.parser.tree.TerminateQueryTest;
import io.confluent.ksql.parser.tree.TumblingWindowExpressionTest;
import io.confluent.ksql.parser.tree.WindowExpressionTest;
import io.confluent.ksql.parser.tree.WithinExpressionTest;
import io.confluent.ksql.schema.ksql.SqlTypeParserTest;
import io.confluent.ksql.util.GrammarTokenExporterTest;
import io.confluent.ksql.util.IdentifierUtilTest;
import io.confluent.ksql.util.ParserKeywordValidatorUtilTest;
import io.confluent.ksql.util.ParserUtilTest;
import io.confluent.ksql.util.QueryGuidTest;
import io.confluent.ksql.util.QueryMaskTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ColumnSerdeTest.class,
        ExpressionDeserializerTest.class,
        ExpressionSerializerTest.class,
        KsqlTypesSerdeModuleTest.class,
        LogicalSchemaDeserializerTest.class,
        SelectExpressionDeserializerTest.class,
        SelectExpressionSerializerTest.class,
        WindowExpressionDeserializerTest.class,
        WindowExpressionSerializerTest.class,
        CreateSourceAsPropertiesTest.class,
        CreateSourcePropertiesTest.class,
        InsertIntoPropertiesTest.class,
        AliasedRelationTest.class,
        AllColumnsTest.class,
        AssertSchemaTest.class,
        AssertStreamTest.class,
        AssertTopicTest.class,
        AssertValuesTest.class,
        CreateConnectorTest.class,
        CreateStreamAsSelectTest.class,
        CreateStreamTest.class,
        CreateTableAsSelectTest.class,
        CreateTableTest.class,
        DescribeConnectorTest.class,
        DropStreamTest.class,
        DropTableTest.class,
        ExplainTest.class,
        GroupByTest.class,
        HoppingWindowExpressionTest.class,
        InsertIntoTest.class,
        InsertValuesTest.class,
        JoinedSourceTest.class,
        JoinTest.class,
        ListConnectorPluginsTest.class,
        ListConnectorsTest.class,
        ListPropertiesTest.class,
        ListQueriesTest.class,
        ListStreamsTest.class,
        ListTablesTest.class,
        ListTopicsTest.class,
        ParserModelTest.class,
        PartitionByTest.class,
        QueryTest.class,
        SessionWindowExpressionTest.class,
        ShowColumnsTest.class,
        SingleColumnTest.class,
        StatementsTest.class,
        StructAllTest.class,
        TableElementsTest.class,
        TableElementTest.class,
        TableTest.class,
        TerminateQueryTest.class,
        TumblingWindowExpressionTest.class,
        WindowExpressionTest.class,
        WithinExpressionTest.class,
        ParserUtilTest.class,
        AssertTableTest.class,
        AstBuilderTest.class,
        ColumnReferenceParserTest.class,
        ExpressionParserTest.class,
        ImmutabilityTest.class,
        KsqlParserTest.class,
        LiteralUtilTest.class,
        ReservedWordMetaTest.class,
        SchemaParserTest.class,
        SqlFormatterTest.class,
        SyntaxErrorValidatorTest.class,
        VariableSubstitutorTest.class,
        SqlTypeParserTest.class,
        GrammarTokenExporterTest.class,
        IdentifierUtilTest.class,
        ParserKeywordValidatorUtilTest.class,
        ParserUtilTest.class,
        QueryGuidTest.class,
        QueryMaskTest.class
})
public class MyTestsParser {
}
