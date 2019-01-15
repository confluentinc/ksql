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

package io.confluent.ksql.benchmark;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.benchmark.SerdeBenchmark.AbstractBenchmark.SchemaAndGenericRowState;
import io.confluent.ksql.benchmark.SerdeBenchmark.AbstractBenchmark.SerdeState;
import io.confluent.ksql.benchmark.SerdeBenchmark.AvroBenchmark.AvroSerdeState;
import io.confluent.ksql.benchmark.SerdeBenchmark.JsonBenchmark.JsonSerdeState;
import java.util.Collection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SerdeBenchmarkTest {

  private static final String topicName = "serde_benchmark";

  private String schemaName;
  private SchemaAndGenericRowState schemaState;

  public SerdeBenchmarkTest(final String schemaName) {
    this.schemaName = schemaName;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    return ImmutableList.of("impressions", "metrics");
  }

  @Before
  public void setUp() throws Exception {
    schemaState = new SchemaAndGenericRowState();
    schemaState.schemaName = schemaName;
    schemaState.setUp();
  }

  @Test
  public void shouldSerializeDeserializeJson() {
    assertSerializeDeserialize(new JsonSerdeState());
  }

  @Test
  public void shouldSerializeDeserializeAvro() {
    assertSerializeDeserialize(new AvroSerdeState());
  }

  private void assertSerializeDeserialize(final SerdeState serdeState) {
    serdeState.setUp(schemaState);

    assertThat(serdeState.serializer.serialize(topicName, serdeState.row), is(serdeState.bytes));
    assertThat(serdeState.deserializer.deserialize(topicName, serdeState.bytes), is(serdeState.row));
  }
}