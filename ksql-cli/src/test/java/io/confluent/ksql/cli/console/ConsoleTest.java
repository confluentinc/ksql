/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import io.confluent.ksql.FakeException;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.TestTerminal;
import io.confluent.ksql.cli.console.Console.NoOpRowCaptor;
import io.confluent.ksql.cli.console.cmd.CliSpecificCommand;
import io.confluent.ksql.rest.entity.ArgumentInfo;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.EntityQueryId;
import io.confluent.ksql.rest.entity.ExecutionPlan;
import io.confluent.ksql.rest.entity.FieldInfo;
import io.confluent.ksql.rest.entity.FunctionDescriptionList;
import io.confluent.ksql.rest.entity.FunctionInfo;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.entity.KafkaTopicInfo;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
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
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.SchemaUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
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
  public void testPrintKSqlEntityList() throws IOException {
    final Map<String, Object> properties = new HashMap<>();
    properties.put("k1", 1);
    properties.put("k2", "v2");
    properties.put("k3", true);

    final List<RunningQuery> queries = new ArrayList<>();
    queries.add(
        new RunningQuery(
            "select * from t1", Collections.singleton("Test"), new EntityQueryId("0")));

    for (int i = 0; i < 5; i++) {
      final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
          new CommandStatusEntity(
              "e",
              CommandId.fromString("topic/1/create"),
              new CommandStatus(CommandStatus.Status.SUCCESS, "Success Message"),
              0L),
          new PropertiesList("e", properties, Collections.emptyList(), Collections.emptyList()),
          new Queries("e", queries),
          new SourceDescriptionEntity(
              "e",
              new SourceDescription(
                  "TestSource", Collections.emptyList(), Collections.emptyList(), buildTestSchema(i),
                  DataSource.DataSourceType.KTABLE.getKqlType(), "key", "2000-01-01", "stats",
                  "errors", false, "avro", "kadka-topic", 1, 1)),
          new TopicDescription("e", "TestTopic", "TestKafkaTopic", "AVRO", "schemaString"),
          new StreamsList("e", ImmutableList.of(new SourceInfo.Stream("TestStream", "TestTopic", "AVRO"))),
          new TablesList("e", ImmutableList.of(new SourceInfo.Table("TestTable", "TestTopic", "JSON", false))),
          new KsqlTopicsList("e", ImmutableList.of(new KsqlTopicInfo("TestTopic", "TestKafkaTopic", DataSource.DataSourceSerDe.JSON))),
          new KafkaTopicsList("e", ImmutableList.of(new KafkaTopicInfo("TestKafkaTopic", true, ImmutableList.of(1),  1, 1))),
          new ExecutionPlan("Test Execution Plan")
      ));
      console.printKsqlEntityList(entityList);
    }
  }

  @Test
  public void shouldPrintTopicDescribeExtended() throws IOException {
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new SourceDescriptionEntity(
            "e",
            new SourceDescription(
                "TestSource", Collections.emptyList(), Collections.emptyList(),
                buildTestSchema(2), DataSource.DataSourceType.KTABLE.getKqlType(),
                "key", "2000-01-01", "stats", "errors", true, "avro", "kadka-topic",
                2, 1))));

    console.printKsqlEntityList(entityList);

    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, containsString("\"topic\" : \"kadka-topic\""));
    } else {
      assertThat(output, containsString("Kafka topic          : kadka-topic (partitions: 2, replication: 1)"));
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
                            + "and stuff"
                        )
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
          + "\tVariation   : FOO(arg1 INT)\n"
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

  private static List<FieldInfo> buildTestSchema(final int size) {
    final SchemaBuilder dataSourceBuilder = SchemaBuilder.struct().name("TestSchema");
    for (int i = 0; i < size; i++) {
      dataSourceBuilder.field("f_" + i, SchemaUtil.getTypeSchema("STRING"));
    }
    return EntityUtil.buildSourceSchemaEntity(dataSourceBuilder.build());
  }
}
