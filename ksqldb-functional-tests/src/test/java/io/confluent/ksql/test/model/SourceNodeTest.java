/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.test.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import java.time.Duration;
import java.util.Optional;
import org.junit.Test;

public class SourceNodeTest {

  static final SourceNode INSTANCE = new SourceNode(
      "bob",
      "stream",
      Optional.of("ROWKEY INT KEY, NAME STRING"),
      Optional.of(KeyFormatNodeTest.INSTANCE),
      Optional.of("JSON"),
      Optional.of(ImmutableSet.of(SerdeFeature.UNWRAP_SINGLES)),
      Optional.of(ImmutableSet.of(SerdeFeature.WRAP_SINGLES)),
      Optional.empty()
  );

  private static final SourceNode INSTANCE_WITHOUT_SERDE_FEATURES = new SourceNode(
      "bob",
      "stream",
      Optional.of("ROWKEY INT KEY, NAME STRING"),
      Optional.of(KeyFormatNodeTest.INSTANCE),
      Optional.of("JSON"),
      Optional.empty(),
      Optional.empty(),
      Optional.empty()
  );

  private static final SourceNode INSTANCE_WITH_EMPTY_SERDE_FEATURES = new SourceNode(
      "bob",
      "stream",
      Optional.of("ROWKEY INT KEY, NAME STRING"),
      Optional.of(KeyFormatNodeTest.INSTANCE),
      Optional.of("JSON"),
      Optional.of(ImmutableSet.of()),
      Optional.of(ImmutableSet.of()),
      Optional.empty()
  );

  private static final SourceNode SOURCE_STREAM_INSTANCE = new SourceNode(
      "bob",
      "stream",
      Optional.of("ROWKEY INT KEY, NAME STRING"),
      Optional.of(KeyFormatNodeTest.INSTANCE),
      Optional.of("JSON"),
      Optional.of(ImmutableSet.of()),
      Optional.of(ImmutableSet.of()),
      Optional.of(true)
  );

  @Test
  public void shouldRoundTrip() {
    ModelTester.assertRoundTrip(INSTANCE);
  }

  @Test
  public void shouldRoundTripWithoutSerdeFeatures() {
    ModelTester.assertRoundTrip(INSTANCE_WITHOUT_SERDE_FEATURES);
  }

  @Test
  public void shouldRoundTripWithEmptySerdeFeatures() {
    ModelTester.assertRoundTrip(INSTANCE_WITH_EMPTY_SERDE_FEATURES);
  }

  @Test
  public void shouldRoundTripSourceStreamInstance() {
    ModelTester.assertRoundTrip(SOURCE_STREAM_INSTANCE);
  }

  @Test
  public void shouldBuildFromDataSource() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("bob"), SqlTypes.BIGINT)
        .build();

    final KsqlTopic topic = mock(KsqlTopic.class);
    when(topic.getKeyFormat()).thenReturn(KeyFormat.windowed(
        FormatInfo.of("AVRO", ImmutableMap.of("some", "prop")),
        SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES),
        WindowInfo.of(WindowType.HOPPING, Optional.of(Duration.ofMillis(10)), Optional.empty())
    ));
    when(topic.getValueFormat()).thenReturn(ValueFormat.of(
        FormatInfo.of("DELIMITED", ImmutableMap.of("some1", "prop1")),
        SerdeFeatures.of(SerdeFeature.WRAP_SINGLES)
    ));

    final DataSource source = mock(DataSource.class);
    when(source.getName()).thenReturn(SourceName.of("the Name"));
    when(source.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(source.getSchema()).thenReturn(schema);
    when(source.getKsqlTopic()).thenReturn(topic);

    // When:
    final SourceNode sourceNode = SourceNode.fromDataSource(source);

    // Then:
    assertThat(sourceNode, is(new SourceNode(
        "the Name",
        "TABLE",
        Optional.of(schema.toString()),
        Optional.of(new KeyFormatNode(
            Optional.of("AVRO"),
            Optional.of(WindowType.HOPPING),
            Optional.of(10L)
        )),
        Optional.of("DELIMITED"),
        Optional.of(ImmutableSet.of(SerdeFeature.UNWRAP_SINGLES)),
        Optional.of(ImmutableSet.of(SerdeFeature.WRAP_SINGLES)),
        Optional.of(false)
    )));
  }
}