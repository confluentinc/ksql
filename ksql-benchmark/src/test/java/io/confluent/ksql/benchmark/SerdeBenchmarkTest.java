/*
 * Copyright 2019 Confluent Inc.
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
import com.google.common.collect.Lists;
import io.confluent.ksql.benchmark.SerdeBenchmark.SchemaAndGenericRowState;
import io.confluent.ksql.benchmark.SerdeBenchmark.SerdeState;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SerdeBenchmarkTest {

  private static final List<String> SCHEMAS = ImmutableList.of("impressions", "metrics");
  private static final List<String> FORMATS = ImmutableList.of("JSON", "Avro");
  private static final String TOPIC_NAME = "serde_benchmark";

  private final String schemaName;
  private final String serializationFormat;

  private SerdeState serdeState;

  public SerdeBenchmarkTest(final String schemaName, final String serializationFormat) {
    this.schemaName = schemaName;
    this.serializationFormat = serializationFormat;
  }

  @Parameterized.Parameters(name = "{0} - {1}")
  public static Iterable<Object[]> data() {
    return Lists.cartesianProduct(SCHEMAS, FORMATS)
        .stream()
        .map(List::toArray)
        .collect(Collectors.toList());
  }

  @Before
  public void setUp() throws Exception {
    final SchemaAndGenericRowState schemaState = new SchemaAndGenericRowState();
    schemaState.schemaName = schemaName;
    schemaState.setUp();

    serdeState = new SerdeState();
    serdeState.serializationFormat = serializationFormat;
    serdeState.setUp(schemaState);
  }

  @Test
  public void shouldSerializeDeserialize() {
    assertThat(serdeState.serializer.serialize(TOPIC_NAME, serdeState.row), is(serdeState.bytes));
    assertThat(serdeState.deserializer.deserialize(TOPIC_NAME, serdeState.bytes), is(serdeState.row));
  }
}