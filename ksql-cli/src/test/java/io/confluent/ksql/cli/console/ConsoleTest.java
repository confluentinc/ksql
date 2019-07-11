/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.cli.console;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.FakeException;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.TestTerminal;
import io.confluent.ksql.cli.console.Console.NoOpRowCaptor;
import io.confluent.ksql.cli.console.cmd.CliSpecificCommand;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.rest.entity.ArgumentInfo;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.EntityQueryId;
import io.confluent.ksql.rest.entity.ExecutionPlan;
import io.confluent.ksql.rest.entity.FieldInfo;
import io.confluent.ksql.rest.entity.FunctionDescriptionList;
import io.confluent.ksql.rest.entity.FunctionInfo;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlTopicInfo;
import io.confluent.ksql.rest.entity.KsqlTopicsList;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.util.EntityUtil;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ConsoleTest {

  private static final String CLI_CMD_NAME = "some command";
  private static final String WHITE_SPACE = " \t ";

  private final TestTerminal terminal;
  private final Console console;
  private final Supplier<String> lineSupplier;
  private final CliSpecificCommand cliCommand;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<OutputFormat> data() {
    return ImmutableList.of(OutputFormat.JSON, OutputFormat.TABULAR);
  }

  @SuppressWarnings("unchecked")
  public ConsoleTest(final OutputFormat outputFormat) {
    this.lineSupplier = mock(Supplier.class);
    this.cliCommand = mock(CliSpecificCommand.class);
    this.terminal = new TestTerminal(lineSupplier);
    this.console = new Console(outputFormat, terminal, new NoOpRowCaptor());

    when(cliCommand.getName()).thenReturn(CLI_CMD_NAME);
    console.registerCliSpecificCommand(cliCommand);
  }

  @After
  public void after() {
    console.close();
  }

  @Test
  public void testPrintGenericStreamedRow() throws IOException {
    final StreamedRow row = StreamedRow.row(new GenericRow(ImmutableList.of("col_1", "col_2")));
    console.printStreamedRow(row);
  }

  @Test
  public void testPrintErrorStreamedRow() throws IOException {
    final FakeException exception = new FakeException();

    console.printStreamedRow(StreamedRow.error(exception));

    assertThat(terminal.getOutputString(), is(exception.getMessage() + "\n"));
  }

  @Test
  public void testPrintFinalMessageStreamedRow() throws IOException {
    console.printStreamedRow(StreamedRow.finalMessage("Some message"));
    assertThat(terminal.getOutputString(), is("Some message\n"));
  }

  @Test
  public void testPrintCommandStatus() throws IOException {
    // Given:
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new CommandStatusEntity(
            "e",
            CommandId.fromString("topic/1/create"),
            new CommandStatus(CommandStatus.Status.SUCCESS, "Success Message"),
            0L)
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {\n"
          + "  \"@type\" : \"currentStatus\",\n"
          + "  \"statementText\" : \"e\",\n"
          + "  \"commandId\" : \"topic/1/create\",\n"
          + "  \"commandStatus\" : {\n"
          + "    \"status\" : \"SUCCESS\",\n"
          + "    \"message\" : \"Success Message\"\n"
          + "  },\n"
          + "  \"commandSequenceNumber\" : 0\n"
          + "} ]\n"));
    } else {
      assertThat(output, is("\n"
          + " Message         \n"
          + "-----------------\n"
          + " Success Message \n"
          + "-----------------\n"));
    }
  }

  @Test
  public void testPrintPropertyList() throws IOException {
    // Given:
    final Map<String, Object> properties = new HashMap<>();
    properties.put("k1", 1);
    properties.put("k2", "v2");
    properties.put("k3", true);

    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new PropertiesList("e", properties, Collections.emptyList(), Collections.emptyList())
      ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {\n"
          + "  \"@type\" : \"properties\",\n"
          + "  \"statementText\" : \"e\",\n"
          + "  \"properties\" : {\n"
          + "    \"k1\" : 1,\n"
          + "    \"k2\" : \"v2\",\n"
          + "    \"k3\" : true\n"
          + "  },\n"
          + "  \"overwrittenProperties\" : [ ],\n"
          + "  \"defaultProperties\" : [ ]\n"
          + "} ]\n"));
    } else {
      assertThat(output, is("\n"
          + " Property | Default override | Effective Value \n"
          + "-----------------------------------------------\n"
          + " k1       | SERVER           | 1               \n"
          + " k2       | SERVER           | v2              \n"
          + " k3       | SERVER           | true            \n"
          + "-----------------------------------------------\n"));
    }
  }

  @Test
  public void testPrintQueries() throws IOException {
    // Given:
    final List<RunningQuery> queries = new ArrayList<>();
    queries.add(
        new RunningQuery(
            "select * from t1", Collections.singleton("Test"), new EntityQueryId("0")));

    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new Queries("e", queries)
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {\n"
          + "  \"@type\" : \"queries\",\n"
          + "  \"statementText\" : \"e\",\n"
          + "  \"queries\" : [ {\n"
          + "    \"sinks\" : [ \"Test\" ],\n"
          + "    \"id\" : \"0\",\n"
          + "    \"queryString\" : \"select * from t1\"\n"
          + "  } ]\n"
          + "} ]\n"));
    } else {
      assertThat(output, is("\n"
          + " Query ID | Kafka Topic | Query String     \n"
          + "-------------------------------------------\n"
          + " 0        | Test        | select * from t1 \n"
          + "-------------------------------------------\n"
          + "For detailed information on a Query run: EXPLAIN <Query ID>;\n"));
    }
  }

  @Test
  public void testPrintSourceDescription() throws IOException {
    // Given:
    final List<FieldInfo> fields = buildTestSchema(
        Schema.OPTIONAL_BOOLEAN_SCHEMA,
        Schema.OPTIONAL_INT32_SCHEMA,
        Schema.OPTIONAL_INT64_SCHEMA,
        Schema.OPTIONAL_FLOAT64_SCHEMA,
        Schema.OPTIONAL_STRING_SCHEMA,
        SchemaBuilder
            .array(Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build(),
        SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build(),
        SchemaBuilder
            .struct()
            .field("a", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .optional()
            .build()
    );

    final List<RunningQuery> readQueries = ImmutableList.of(
        new RunningQuery("read query", ImmutableSet.of("sink1"), new EntityQueryId("readId"))
    );
    final List<RunningQuery> writeQueries = ImmutableList.of(
        new RunningQuery("write query", ImmutableSet.of("sink2"), new EntityQueryId("writeId"))
    );

    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new SourceDescriptionEntity(
            "some sql",
            new SourceDescription(
                "TestSource", readQueries, writeQueries, fields,
                DataSourceType.KTABLE.getKsqlType(), "key", "2000-01-01", "stats",
                "errors", false, "avro", "kadka-topic", 1, 1))
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {\n"
          + "  \"@type\" : \"sourceDescription\",\n"
          + "  \"statementText\" : \"some sql\",\n"
          + "  \"sourceDescription\" : {\n"
          + "    \"name\" : \"TestSource\",\n"
          + "    \"readQueries\" : [ {\n"
          + "      \"sinks\" : [ \"sink1\" ],\n"
          + "      \"id\" : \"readId\",\n"
          + "      \"queryString\" : \"read query\"\n"
          + "    } ],\n"
          + "    \"writeQueries\" : [ {\n"
          + "      \"sinks\" : [ \"sink2\" ],\n"
          + "      \"id\" : \"writeId\",\n"
          + "      \"queryString\" : \"write query\"\n"
          + "    } ],\n"
          + "    \"fields\" : [ {\n"
          + "      \"name\" : \"ROWTIME\",\n"
          + "      \"schema\" : {\n"
          + "        \"type\" : \"BIGINT\",\n"
          + "        \"fields\" : null,\n"
          + "        \"memberSchema\" : null\n"
          + "      }\n"
          + "    }, {\n"
          + "      \"name\" : \"ROWKEY\",\n"
          + "      \"schema\" : {\n"
          + "        \"type\" : \"STRING\",\n"
          + "        \"fields\" : null,\n"
          + "        \"memberSchema\" : null\n"
          + "      }\n"
          + "    }, {\n"
          + "      \"name\" : \"f_0\",\n"
          + "      \"schema\" : {\n"
          + "        \"type\" : \"BOOLEAN\",\n"
          + "        \"fields\" : null,\n"
          + "        \"memberSchema\" : null\n"
          + "      }\n"
          + "    }, {\n"
          + "      \"name\" : \"f_1\",\n"
          + "      \"schema\" : {\n"
          + "        \"type\" : \"INTEGER\",\n"
          + "        \"fields\" : null,\n"
          + "        \"memberSchema\" : null\n"
          + "      }\n"
          + "    }, {\n"
          + "      \"name\" : \"f_2\",\n"
          + "      \"schema\" : {\n"
          + "        \"type\" : \"BIGINT\",\n"
          + "        \"fields\" : null,\n"
          + "        \"memberSchema\" : null\n"
          + "      }\n"
          + "    }, {\n"
          + "      \"name\" : \"f_3\",\n"
          + "      \"schema\" : {\n"
          + "        \"type\" : \"DOUBLE\",\n"
          + "        \"fields\" : null,\n"
          + "        \"memberSchema\" : null\n"
          + "      }\n"
          + "    }, {\n"
          + "      \"name\" : \"f_4\",\n"
          + "      \"schema\" : {\n"
          + "        \"type\" : \"STRING\",\n"
          + "        \"fields\" : null,\n"
          + "        \"memberSchema\" : null\n"
          + "      }\n"
          + "    }, {\n"
          + "      \"name\" : \"f_5\",\n"
          + "      \"schema\" : {\n"
          + "        \"type\" : \"ARRAY\",\n"
          + "        \"fields\" : null,\n"
          + "        \"memberSchema\" : {\n"
          + "          \"type\" : \"STRING\",\n"
          + "          \"fields\" : null,\n"
          + "          \"memberSchema\" : null\n"
          + "        }\n"
          + "      }\n"
          + "    }, {\n"
          + "      \"name\" : \"f_6\",\n"
          + "      \"schema\" : {\n"
          + "        \"type\" : \"MAP\",\n"
          + "        \"fields\" : null,\n"
          + "        \"memberSchema\" : {\n"
          + "          \"type\" : \"BIGINT\",\n"
          + "          \"fields\" : null,\n"
          + "          \"memberSchema\" : null\n"
          + "        }\n"
          + "      }\n"
          + "    }, {\n"
          + "      \"name\" : \"f_7\",\n"
          + "      \"schema\" : {\n"
          + "        \"type\" : \"STRUCT\",\n"
          + "        \"fields\" : [ {\n"
          + "          \"name\" : \"a\",\n"
          + "          \"schema\" : {\n"
          + "            \"type\" : \"DOUBLE\",\n"
          + "            \"fields\" : null,\n"
          + "            \"memberSchema\" : null\n"
          + "          }\n"
          + "        } ],\n"
          + "        \"memberSchema\" : null\n"
          + "      }\n"
          + "    } ],\n"
          + "    \"type\" : \"TABLE\",\n"
          + "    \"key\" : \"key\",\n"
          + "    \"timestamp\" : \"2000-01-01\",\n"
          + "    \"statistics\" : \"stats\",\n"
          + "    \"errorStats\" : \"errors\",\n"
          + "    \"extended\" : false,\n"
          + "    \"format\" : \"avro\",\n"
          + "    \"topic\" : \"kadka-topic\",\n"
          + "    \"partitions\" : 1,\n"
          + "    \"replication\" : 1\n"
          + "  }\n"
          + "} ]\n"));
    } else {
      assertThat(output, is("\n"
          + "Name                 : TestSource\n"
          + " Field   | Type                      \n"
          + "-------------------------------------\n"
          + " ROWTIME | BIGINT           (system) \n"
          + " ROWKEY  | VARCHAR(STRING)  (system) \n"
          + " f_0     | BOOLEAN                   \n"
          + " f_1     | INTEGER                   \n"
          + " f_2     | BIGINT                    \n"
          + " f_3     | DOUBLE                    \n"
          + " f_4     | VARCHAR(STRING)           \n"
          + " f_5     | ARRAY<VARCHAR(STRING)>    \n"
          + " f_6     | MAP<STRING, BIGINT>       \n"
          + " f_7     | STRUCT<a DOUBLE>          \n"
          + "-------------------------------------\n"
          + "For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;\n"));
    }
  }

  @Test
  public void testPrintTopicDescription() throws IOException {
    // Given:
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new TopicDescription("e", "TestTopic", "TestKafkaTopic", "AVRO", "schemaString")
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {\n"
          + "  \"@type\" : \"topicDescription\",\n"
          + "  \"statementText\" : \"e\",\n"
          + "  \"name\" : \"TestTopic\",\n"
          + "  \"format\" : \"AVRO\",\n"
          + "  \"schemaString\" : \"schemaString\",\n"
          + "  \"kafkaTopic\" : \"TestKafkaTopic\"\n"
          + "} ]\n"));
    } else {
      assertThat(output, is("\n"
          + " Table Name | Kafka Topic    | Type | AvroSchema   \n"
          + "---------------------------------------------------\n"
          + " TestTopic  | TestKafkaTopic | AVRO | schemaString \n"
          + "---------------------------------------------------\n"));
    }
  }

  @Test
  public void testPrintStreamsList() throws IOException {
    // Given:
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new StreamsList("e",
            ImmutableList.of(new SourceInfo.Stream("TestStream", "TestTopic", "AVRO")))
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {\n"
          + "  \"@type\" : \"streams\",\n"
          + "  \"statementText\" : \"e\",\n"
          + "  \"streams\" : [ {\n"
          + "    \"type\" : \"STREAM\",\n"
          + "    \"name\" : \"TestStream\",\n"
          + "    \"topic\" : \"TestTopic\",\n"
          + "    \"format\" : \"AVRO\"\n"
          + "  } ]\n"
          + "} ]\n"));
    } else {
      assertThat(output, is("\n"
          + " Stream Name | Kafka Topic | Format \n"
          + "------------------------------------\n"
          + " TestStream  | TestTopic   | AVRO   \n"
          + "------------------------------------\n"));
    }
  }

  @Test
  public void testSortedPrintStreamsList() throws IOException {
    // Given:
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
            new StreamsList("e",
                    ImmutableList.of(
                            new SourceInfo.Stream("B", "TestTopic", "AVRO"),
                            new SourceInfo.Stream("A", "TestTopic", "AVRO"),
                            new SourceInfo.Stream("Z", "TestTopic", "AVRO"),
                            new SourceInfo.Stream("C", "TestTopic", "AVRO")
                    ))
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {\n"
               + "  \"@type\" : \"streams\",\n"
               + "  \"statementText\" : \"e\",\n"
               + "  \"streams\" : [ {\n"
               + "    \"type\" : \"STREAM\",\n"
               + "    \"name\" : \"B\",\n"
               + "    \"topic\" : \"TestTopic\",\n"
               + "    \"format\" : \"AVRO\"\n"
               + "  }, {\n"
               + "    \"type\" : \"STREAM\",\n"
               + "    \"name\" : \"A\",\n"
               + "    \"topic\" : \"TestTopic\",\n"
               + "    \"format\" : \"AVRO\"\n"
               + "  }, {\n"
               + "    \"type\" : \"STREAM\",\n"
               + "    \"name\" : \"Z\",\n"
               + "    \"topic\" : \"TestTopic\",\n"
               + "    \"format\" : \"AVRO\"\n"
               + "  }, {\n"
               + "    \"type\" : \"STREAM\",\n"
               + "    \"name\" : \"C\",\n"
               + "    \"topic\" : \"TestTopic\",\n"
               + "    \"format\" : \"AVRO\"\n"
               + "  } ]\n"
               +"} ]\n"));
    } else {
      assertThat(output, is("\n"
              + " Stream Name | Kafka Topic | Format \n"
              + "------------------------------------\n"
              + " A           | TestTopic   | AVRO   \n"
              + " B           | TestTopic   | AVRO   \n"
              + " C           | TestTopic   | AVRO   \n"
              + " Z           | TestTopic   | AVRO   \n"
              + "------------------------------------\n"));
    }
  }

  @Test
  public void testPrintTablesList() throws IOException {
    // Given:
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new TablesList("e",
            ImmutableList.of(new SourceInfo.Table("TestTable", "TestTopic", "JSON", false)))
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {\n"
          + "  \"@type\" : \"tables\",\n"
          + "  \"statementText\" : \"e\",\n"
          + "  \"tables\" : [ {\n"
          + "    \"type\" : \"TABLE\",\n"
          + "    \"name\" : \"TestTable\",\n"
          + "    \"topic\" : \"TestTopic\",\n"
          + "    \"format\" : \"JSON\",\n"
          + "    \"isWindowed\" : false\n"
          + "  } ]\n"
          + "} ]\n"));
    } else {
      assertThat(output, is("\n"
          + " Table Name | Kafka Topic | Format | Windowed \n"
          + "----------------------------------------------\n"
          + " TestTable  | TestTopic   | JSON   | false    \n"
          + "----------------------------------------------\n"));
    }
  }

  @Test
  public void testSortedPrintTablesList() throws IOException {
    // Given:
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
            new TablesList("e",
                    ImmutableList.of(
                            new SourceInfo.Table("B", "TestTopic", "JSON", false),
                            new SourceInfo.Table("A", "TestTopic", "JSON", false),
                            new SourceInfo.Table("Z", "TestTopic", "JSON", false),
                            new SourceInfo.Table("C", "TestTopic", "JSON", false)
                    )
            )
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {\n"
              + "  \"@type\" : \"tables\",\n"
              + "  \"statementText\" : \"e\",\n"
              + "  \"tables\" : [ {\n"
              + "    \"type\" : \"TABLE\",\n"
              + "    \"name\" : \"B\",\n"
              + "    \"topic\" : \"TestTopic\",\n"
              + "    \"format\" : \"JSON\",\n"
              + "    \"isWindowed\" : false\n"
              + "  }, {\n"
              + "    \"type\" : \"TABLE\",\n"
              + "    \"name\" : \"A\",\n"
              + "    \"topic\" : \"TestTopic\",\n"
              + "    \"format\" : \"JSON\",\n"
              + "    \"isWindowed\" : false\n"
              + "  }, {\n"
              + "    \"type\" : \"TABLE\",\n"
              + "    \"name\" : \"Z\",\n"
              + "    \"topic\" : \"TestTopic\",\n"
              + "    \"format\" : \"JSON\",\n"
              + "    \"isWindowed\" : false\n"
              + "  }, {\n"
              + "    \"type\" : \"TABLE\",\n"
              + "    \"name\" : \"C\",\n"
              + "    \"topic\" : \"TestTopic\",\n"
              + "    \"format\" : \"JSON\",\n"
              + "    \"isWindowed\" : false\n"
              + "  } ]\n"
              + "} ]\n"));
    } else {
      assertThat(output, is("\n"
              + " Table Name | Kafka Topic | Format | Windowed \n"
              + "----------------------------------------------\n"
              + " A          | TestTopic   | JSON   | false    \n"
              + " B          | TestTopic   | JSON   | false    \n"
              + " C          | TestTopic   | JSON   | false    \n"
              + " Z          | TestTopic   | JSON   | false    \n"
              + "----------------------------------------------\n"));
    }
  }

  @Test
  public void testPrintTopicsList() throws IOException {
    // Given:
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new KsqlTopicsList("e",
            ImmutableList.of(new KsqlTopicInfo("TestTopic", "TestKafkaTopic", Format.JSON)))
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {\n"
          + "  \"@type\" : \"ksql_topics\",\n"
          + "  \"statementText\" : \"e\",\n"
          + "  \"topics\" : [ {\n"
          + "    \"name\" : \"TestTopic\",\n"
          + "    \"kafkaTopic\" : \"TestKafkaTopic\",\n"
          + "    \"format\" : \"JSON\"\n"
          + "  } ]\n"
          + "} ]\n"));
    } else {
      assertThat(output, is("\n"
          + " Ksql Topic | Kafka Topic    | Format \n"
          + "--------------------------------------\n"
          + " TestTopic  | TestKafkaTopic | JSON   \n"
          + "--------------------------------------\n"));
    }
  }

  @Test
  public void testPrintExecuptionPlan() throws IOException {
    // Given:
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new ExecutionPlan("Test Execution Plan")
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {\n"
          + "  \"@type\" : \"executionPlan\",\n"
          + "  \"statementText\" : \"Test Execution Plan\",\n"
          + "  \"executionPlan\" : \"Test Execution Plan\"\n"
          + "} ]\n"));
    } else {
      assertThat(output, is("\n"
          + " Execution Plan      \n"
          + "---------------------\n"
          + " Test Execution Plan \n"
          + "---------------------\n"));
    }
  }

  @Test
  public void shouldPrintTopicDescribeExtended() throws IOException {
    // Given:
    final List<RunningQuery> readQueries = ImmutableList.of(
        new RunningQuery("read query", ImmutableSet.of("sink1"), new EntityQueryId("readId"))
    );
    final List<RunningQuery> writeQueries = ImmutableList.of(
        new RunningQuery("write query", ImmutableSet.of("sink2"), new EntityQueryId("writeId"))
    );

    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new SourceDescriptionEntity(
            "e",
            new SourceDescription(
                "TestSource", readQueries, writeQueries,
                buildTestSchema(Schema.OPTIONAL_STRING_SCHEMA), DataSourceType.KTABLE.getKsqlType(),
                "key", "2000-01-01", "stats", "errors", true, "avro", "kadka-topic",
                2, 1))));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {\n"
          + "  \"@type\" : \"sourceDescription\",\n"
          + "  \"statementText\" : \"e\",\n"
          + "  \"sourceDescription\" : {\n"
          + "    \"name\" : \"TestSource\",\n"
          + "    \"readQueries\" : [ {\n"
          + "      \"sinks\" : [ \"sink1\" ],\n"
          + "      \"id\" : \"readId\",\n"
          + "      \"queryString\" : \"read query\"\n"
          + "    } ],\n"
          + "    \"writeQueries\" : [ {\n"
          + "      \"sinks\" : [ \"sink2\" ],\n"
          + "      \"id\" : \"writeId\",\n"
          + "      \"queryString\" : \"write query\"\n"
          + "    } ],\n"
          + "    \"fields\" : [ {\n"
          + "      \"name\" : \"ROWTIME\",\n"
          + "      \"schema\" : {\n"
          + "        \"type\" : \"BIGINT\",\n"
          + "        \"fields\" : null,\n"
          + "        \"memberSchema\" : null\n"
          + "      }\n"
          + "    }, {\n"
          + "      \"name\" : \"ROWKEY\",\n"
          + "      \"schema\" : {\n"
          + "        \"type\" : \"STRING\",\n"
          + "        \"fields\" : null,\n"
          + "        \"memberSchema\" : null\n"
          + "      }\n"
          + "    }, {\n"
          + "      \"name\" : \"f_0\",\n"
          + "      \"schema\" : {\n"
          + "        \"type\" : \"STRING\",\n"
          + "        \"fields\" : null,\n"
          + "        \"memberSchema\" : null\n"
          + "      }\n"
          + "    } ],\n"
          + "    \"type\" : \"TABLE\",\n"
          + "    \"key\" : \"key\",\n"
          + "    \"timestamp\" : \"2000-01-01\",\n"
          + "    \"statistics\" : \"stats\",\n"
          + "    \"errorStats\" : \"errors\",\n"
          + "    \"extended\" : true,\n"
          + "    \"format\" : \"avro\",\n"
          + "    \"topic\" : \"kadka-topic\",\n"
          + "    \"partitions\" : 2,\n"
          + "    \"replication\" : 1\n"
          + "  }\n"
          + "} ]\n"));
    } else {
      assertThat(output, is("\n"
          + "Name                 : TestSource\n"
          + "Type                 : TABLE\n"
          + "Key field            : key\n"
          + "Key format           : STRING\n"
          + "Timestamp field      : 2000-01-01\n"
          + "Value format         : avro\n"
          + "Kafka topic          : kadka-topic (partitions: 2, replication: 1)\n"
          + "\n"
          + " Field   | Type                      \n"
          + "-------------------------------------\n"
          + " ROWTIME | BIGINT           (system) \n"
          + " ROWKEY  | VARCHAR(STRING)  (system) \n"
          + " f_0     | VARCHAR(STRING)           \n"
          + "-------------------------------------\n"
          + "\n"
          + "Queries that read from this TABLE\n"
          + "-----------------------------------\n"
          + "readId : read query\n"
          + "\n"
          + "For query topology and execution plan please run: EXPLAIN <QueryId>\n"
          + "\n"
          + "Queries that write from this TABLE\n"
          + "-----------------------------------\n"
          + "writeId : write query\n"
          + "\n"
          + "For query topology and execution plan please run: EXPLAIN <QueryId>\n"
          + "\n"
          + "Local runtime statistics\n"
          + "------------------------\n"
          + "stats\n"
          + "errors\n"
          + "(Statistics of the local KSQL server interaction with the Kafka topic kadka-topic)\n"));
    }
  }

  @Test
  public void shouldPrintFunctionDescription() throws IOException {
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new FunctionDescriptionList(
            "DESCRIBE FUNCTION foo;",
            "FOO",
            "Description that is very, very, very, very, very, very, very, very, very, "
                + "very, very, very, very, very, very, very, very, very, very, very, very long\n"
                + "and containing new lines\n"
                + "\tAND TABS\n"
                + "too!",
            "Andy",
            "v1.1.0",
            "some.jar",
            ImmutableList.of(new FunctionInfo(
                ImmutableList.of(
                    new ArgumentInfo(
                        "arg1",
                        "INT",
                        "Another really, really, really, really, really, really, really,"
                            + "really, really, really, really, really, really, really, really "
                            + " really, really, really, really, really, really, really, long\n"
                            + "description\n"
                            + "\tContaining Tabs\n"
                            + "and stuff",
                        true)
                ),
                "LONG",
                "The function description, which too can be really, really, really, "
                    + "really, really, really, really, really, really, really, really, really, "
                    + "really, really, really, really, really, really, really, really, long\n"
                    + "and contains\n\ttabs and stuff"
            )), FunctionType.scalar)));

    console.printKsqlEntityList(entityList);

    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, containsString("\"name\" : \"FOO\""));
    } else {
      final String expected = ""
          + "Name        : FOO\n"
          + "Author      : Andy\n"
          + "Version     : v1.1.0\n"
          + "Overview    : Description that is very, very, very, very, very, very, very, very, very, very, very, \n"
          + "              very, very, very, very, very, very, very, very, very, very long\n"
          + "              and containing new lines\n"
          + "              \tAND TABS\n"
          + "              too!\n"
          + "Type        : scalar\n"
          + "Jar         : some.jar\n"
          + "Variations  : \n"
          + "\n"
          + "\tVariation   : FOO(arg1 INT[])\n"
          + "\tReturns     : LONG\n"
          + "\tDescription : The function description, which too can be really, really, really, really, really, \n"
          + "                really, really, really, really, really, really, really, really, really, really, \n"
          + "                really, really, really, really, really, long\n"
          + "                and contains\n"
          + "                \ttabs and stuff\n"
          + "\targ1        : Another really, really, really, really, really, really, really,really, really, \n"
          + "                really, really, really, really, really, really  really, really, really, really, \n"
          + "                really, really, really, long\n"
          + "                description\n"
          + "                \tContaining Tabs\n"
          + "                and stuff";

      assertThat(output, containsString(expected));
    }
  }

  @Test
  public void shouldExecuteCliCommands() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME)
        .thenReturn("not a CLI command;");

    // When:
    console.readLine();

    // Then:
    verify(cliCommand).execute(eq(ImmutableList.of()), any());
  }

  @Test
  public void shouldExecuteCliCommandWithArgsTrimmingWhiteSpace() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME + WHITE_SPACE + "Arg0" + WHITE_SPACE + "Arg1" + WHITE_SPACE)
        .thenReturn("not a CLI command;");

    // When:
    console.readLine();

    // Then:
    verify(cliCommand).execute(eq(ImmutableList.of("Arg0", "Arg1")), any());
  }

  @Test
  public void shouldExecuteCliCommandWithQuotedArgsContainingSpaces() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME + WHITE_SPACE + "Arg0" + WHITE_SPACE + "'Arg 1'")
        .thenReturn("not a CLI command;");

    // When:
    console.readLine();

    // Then:
    verify(cliCommand).execute(eq(ImmutableList.of("Arg0", "Arg 1")), any());
  }

  @Test
  public void shouldSupportOtherWhitespaceBetweenCliCommandAndArgs() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME + "\tArg0" + WHITE_SPACE + "'Arg 1'")
        .thenReturn("not a CLI command;");

    // When:
    console.readLine();

    // Then:
    verify(cliCommand).execute(eq(ImmutableList.of("Arg0", "Arg 1")), any());
  }

  @Test
  public void shouldSupportCmdBeingTerminatedWithSemiColon() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME + WHITE_SPACE  + "Arg0;")
        .thenReturn("not a CLI command;");

    // When:
    console.readLine();

    // Then:
    verify(cliCommand).execute(eq(ImmutableList.of("Arg0")), any());
  }

  @Test
  public void shouldSupportCmdWithQuotedArgBeingTerminatedWithSemiColon() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME + WHITE_SPACE  + "'Arg0';")
        .thenReturn("not a CLI command;");

    // When:
    console.readLine();

    // Then:
    verify(cliCommand).execute(eq(ImmutableList.of("Arg0")), any());
  }

  @Test
  public void shouldFailIfCommandNameIsQuoted() {
    // Given:
    when(lineSupplier.get())
        .thenReturn("'some' 'command' " + "Arg0" + WHITE_SPACE + "'Arg 1'")
        .thenReturn("not a CLI command;");

    // When:
    console.readLine();

    // Then:
    verify(cliCommand, never()).execute(any(), any());
  }

  @Test
  public void shouldSwallowCliCommandLines() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME)
        .thenReturn("not a CLI command;");

    // When:
    final String result = console.readLine();

    // Then:
    assertThat(result, is("not a CLI command;"));
  }

  @Test
  public void shouldSwallowCliCommandLinesEvenWithWhiteSpace() {
    // Given:
    when(lineSupplier.get())
        .thenReturn("   \t   " + CLI_CMD_NAME + "   \t   ")
        .thenReturn("not a CLI command;");

    // When:
    final String result = console.readLine();

    // Then:
    assertThat(result, is("not a CLI command;"));
  }

  private static List<FieldInfo> buildTestSchema(final Schema... fieldTypes) {
    final SchemaBuilder dataSourceBuilder = SchemaBuilder.struct().name("TestSchema");

    for (int idx = 0; idx < fieldTypes.length; idx++) {
      dataSourceBuilder.field("f_" + idx, fieldTypes[idx]);
    }

    return EntityUtil.buildSourceSchemaEntity(LogicalSchema.of(dataSourceBuilder.build()), false);
  }
}
