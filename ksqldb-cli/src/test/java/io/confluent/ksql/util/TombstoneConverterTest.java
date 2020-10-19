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

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.StreamedRow.DataRow;
import io.confluent.ksql.rest.entity.StreamedRow.Header;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TombstoneConverterTest {

  private static final String TOMBSTONE = "<TOMBSTONE>";

  private static final LogicalSchema COLUMN_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("V0"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("V1"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("V2"), SqlTypes.INTEGER)
      .build();

  @Mock
  private Header header;
  @Mock
  private DataRow dataRow;

  private TombstoneConverter converter;

  @Before
  public void setUp() {
    when(header.getColumnsSchema()).thenReturn(COLUMN_SCHEMA);

    when(header.getKeySchema()).thenReturn(Optional.of(ImmutableList.of(
        Column.of(ColumnName.of("V0"), SqlTypes.INTEGER, Namespace.VALUE, 0)
    )));

    when(dataRow.getKey()).thenReturn(Optional.of(ImmutableList.of(10)));
    when(dataRow.getTombstone()).thenReturn(Optional.of(true));
  }

  @Test
  public void shouldThrowIfHeaderNotForTable() {
    // Given:
    when(header.getKeySchema()).thenReturn(Optional.empty());

    // When/Then:
    assertThrows(
        IllegalArgumentException.class,
        () -> new TombstoneConverter(header)
    );
  }

  @Test
  public void shouldThrowIfDataRowNotATombstone() {
    // Given:
    when(dataRow.getTombstone()).thenReturn(Optional.empty());

    final TombstoneConverter converter = new TombstoneConverter(header);

    // When/Then:
    assertThrows(
        IllegalArgumentException.class,
        () -> converter.asColumns(dataRow)
    );
  }

  @Test
  public void shouldThrowIfDataRowNotForTable() {
    // Given:
    when(dataRow.getKey()).thenReturn(Optional.empty());

    final TombstoneConverter converter = new TombstoneConverter(header);

    // When/Then:
    assertThrows(
        IllegalArgumentException.class,
        () -> converter.asColumns(dataRow)
    );
  }

  @Test
  public void shouldHandleKeyAtStartOfProjection() {
    // Given:
    givenHeaderKey("V0");
    givenRowKey(10);

    // When:
    final List<?> result = converter.asColumns(dataRow);

    // Then:
    assertThat(result, is(Arrays.asList(10, TOMBSTONE, TOMBSTONE)));
  }

  @Test
  public void shouldHandleKeyInProjection() {
    // Given:
    givenHeaderKey("V1");
    givenRowKey(11);

    // When:
    final List<?> result = converter.asColumns(dataRow);

    // Then:
    assertThat(result, is(Arrays.asList(TOMBSTONE, 11, TOMBSTONE)));
  }

  @Test
  public void shouldHandleKeyAtEndOfProjection() {
    // Given:
    givenHeaderKey("V2");
    givenRowKey(12);

    // When:
    final List<?> result = converter.asColumns(dataRow);

    // Then:
    assertThat(result, is(Arrays.asList(TOMBSTONE, TOMBSTONE, 12)));
  }

  @Test
  public void shouldHandleMultipleKeyColumnsInProjection() {
    // Given:
    givenHeaderKey("V0", "V2");
    givenRowKey("the", "key");

    // When:
    final List<?> result = converter.asColumns(dataRow);

    // Then:
    assertThat(result, is(Arrays.asList("the", TOMBSTONE, "key")));
  }

  @Test
  public void shouldHandleKeyColumnNotInProjection() {
    // Given:
    givenHeaderKey("K0");
    givenRowKey("what-eva");

    // When:
    final List<?> result = converter.asColumns(dataRow);

    // Then:
    assertThat(result, is(Arrays.asList(TOMBSTONE, TOMBSTONE, TOMBSTONE)));
  }

  @Test
  public void shouldHandleMultipleKeyColumnsNotInProjection() {
    // Given:
    givenHeaderKey("K0", "K1");
    givenRowKey("what", "eva");

    // When:
    final List<?> result = converter.asColumns(dataRow);

    // Then:
    assertThat(result, is(Arrays.asList(TOMBSTONE, TOMBSTONE, TOMBSTONE)));
  }

  @Test
  public void shouldHandleSomeKeyColumnsNotInProjection() {
    // Given:
    givenHeaderKey("K0", "V1");
    givenRowKey("what", "eva");

    // When:
    final List<?> result = converter.asColumns(dataRow);

    // Then:
    assertThat(result, is(Arrays.asList(TOMBSTONE, "eva", TOMBSTONE)));
  }

  private void givenHeaderKey(final String... keyColumnNames) {
    final List<SimpleColumn> key = Arrays.stream(keyColumnNames)
        .map(ColumnName::of)
        .map(cn -> Column.of(cn, SqlTypes.INTEGER, Namespace.VALUE, 0))
        .collect(Collectors.toList());

    when(header.getKeySchema()).thenReturn(Optional.of(key));

    converter = new TombstoneConverter(header);
  }

  private void givenRowKey(final Object... keyData) {
    final List<?> key = Arrays.asList(keyData);
    when(dataRow.getKey()).thenReturn(Optional.of(key));
  }
}