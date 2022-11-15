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

package io.confluent.ksql.rest.server.resources.streaming;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TombstoneFactoryTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K1"), SqlTypes.INTEGER)
      .keyColumn(ColumnName.of("K2"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("V0"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("K2"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("V1"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("K1"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("V2"), SqlTypes.INTEGER)
      .build();

  @Mock
  private TransientQueryMetadata query;

  private TombstoneFactory factory;

  @Before
  public void setUp() {
    givenSchema(SCHEMA);
  }

  @Test
  public void shouldThrowIfDataRowNotATombstone() {
    // Given:
    final KeyValue<List<?>, GenericRow> kv = KeyValue.keyValue(
        ImmutableList.of(4, 2),
        genericRow(1, 2, 3, 4, 5)
    );

    // When/Then:
    assertThrows(
        IllegalArgumentException.class,
        () -> factory.createRow(kv)
    );
  }

  @Test
  public void shouldThrowIfTombstoneKeyHasTooFewColumns() {
    // Given:
    final KeyValue<List<?>, GenericRow> kv = KeyValue.keyValue(
        ImmutableList.of(4),
        null
    );

    // When/Then:
    assertThrows(
        IllegalArgumentException.class,
        () -> factory.createRow(kv)
    );
  }

  @Test
  public void shouldHandleKeyInProjection() {
    // Given:
    final KeyValue<List<?>, GenericRow> kv = KeyValue.keyValue(
        ImmutableList.of(4, 2),
        null
    );

    // When:
    final GenericRow result = factory.createRow(kv);

    // Then:
    assertThat(result, is(genericRow(null, 2, null, 4, null)));
  }

  @Test
  public void shouldHandleKeyColumnNotInProjection() {
    // Given:
    givenSchema(LogicalSchema.builder()
        .keyColumn(ColumnName.of("K1"), SqlTypes.INTEGER)
        .keyColumn(ColumnName.of("K2"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("V0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("V1"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("V2"), SqlTypes.INTEGER)
        .build());

    final KeyValue<List<?>, GenericRow> kv = KeyValue.keyValue(
        ImmutableList.of(4),
        null
    );

    // When:
    final GenericRow result = factory.createRow(kv);

    // Then:
    assertThat(result, is(genericRow(null, null, null)));
  }

  @Test
  public void shouldHandleSomeKeyColumnsNotInProjection() {
    // Given:
    givenSchema(LogicalSchema.builder()
        .keyColumn(ColumnName.of("K1"), SqlTypes.INTEGER)
        .keyColumn(ColumnName.of("K2"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("V0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("K2"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("V1"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("V2"), SqlTypes.INTEGER)
        .build());

    final KeyValue<List<?>, GenericRow> kv = KeyValue.keyValue(
        ImmutableList.of(10, 2),
        null
    );

    // When:
    final GenericRow result = factory.createRow(kv);

    // Then:
    assertThat(result, is(genericRow(null, 2, null, null)));
  }

  private void givenSchema(final LogicalSchema schema) {
    factory = TombstoneFactory.create(schema, ResultType.TABLE);
  }
}