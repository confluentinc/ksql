/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.cli.console;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.FakeException;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.TestTerminal;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.EntityQueryId;
import io.confluent.ksql.rest.entity.ExecutionPlan;
import io.confluent.ksql.rest.entity.FieldInfo;
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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ConsoleTest {

  private TestTerminal terminal;
  private KsqlRestClient client;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<OutputFormat> data() {
    return ImmutableList.of(OutputFormat.JSON, OutputFormat.TABULAR);
  }

  public ConsoleTest(final OutputFormat outputFormat) {
    this.client = new KsqlRestClient("http://localhost:59098");
    this.terminal = new TestTerminal(outputFormat, client);
  }

  @After
  public void after() {
    client.close();
    terminal.close();
  }

  @Test
  public void testPrintGenericStreamedRow() throws IOException {
    final StreamedRow row = StreamedRow.row(new GenericRow(ImmutableList.of("col_1", "col_2")));
    terminal.printStreamedRow(row);
  }

  @Test
  public void testPrintErrorStreamedRow() throws IOException {
    final FakeException exception = new FakeException();

    terminal.printStreamedRow(StreamedRow.error(exception));

    assertThat(terminal.getOutputString(), is(exception.getMessage() + "\n"));
  }

  @Test
  public void testPrintFinalMessageStreamedRow() throws IOException {
    terminal.printStreamedRow(StreamedRow.finalMessage("Some message"));
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
          new CommandStatusEntity("e", "topic/1/create", "SUCCESS", "Success Message"),
          new PropertiesList("e", properties, Collections.emptyList()),
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
      terminal.printKsqlEntityList(entityList);
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

    terminal.printKsqlEntityList(entityList);

    final String output = terminal.getOutputString();
    if (terminal.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, containsString("\"topic\" : \"kadka-topic\""));
    } else {
      assertThat(output, containsString("Kafka topic          : kadka-topic (partitions: 2, replication: 1)"));
    }
  }

  private List<FieldInfo> buildTestSchema(final int size) {
    final SchemaBuilder dataSourceBuilder = SchemaBuilder.struct().name("TestSchema");
    for (int i = 0; i < size; i++) {
      dataSourceBuilder.field("f_" + i, SchemaUtil.getTypeSchema("STRING"));
    }
    return EntityUtil.buildSourceSchemaEntity(dataSourceBuilder.build());
  }
}
