/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.engine.generic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GenericRecordFactoryTest {

  private static final ColumnName KEY = ColumnName.of("K0");
  private static final ColumnName COL0 = ColumnName.of("COL0");
  private static final ColumnName COL1 = ColumnName.of("COL1");

  private final AtomicLong clock = new AtomicLong();
  private final FunctionRegistry functions = TestFunctionRegistry.INSTANCE.get();

  @Mock
  private KsqlConfig ksqlConfig;

  private GenericRecordFactory recordFactory;

  @Before
  public void setUp() {
    clock.set(0L);
    recordFactory = new GenericRecordFactory(ksqlConfig, functions, clock::get);
  }

  @Test
  public void shouldBuildExpression() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.STRING)
        .build();
    final List<ColumnName> names = ImmutableList.of(KEY, COL0);
    final Expression exp = new FunctionCall(
        FunctionName.of("CONCAT"),
        ImmutableList.of(new StringLiteral("a"), new StringLiteral("b"))
    );

    // When:
    final KsqlGenericRecord record = recordFactory.build(
        names, ImmutableList.of(exp, exp), schema, DataSourceType.KSTREAM
    );

    // Then:
    assertThat(record, is(KsqlGenericRecord.of(
        GenericKey.genericKey("ab"),
        GenericRow.genericRow("ab"),
        0
    )));
  }

  @Test
  public void shouldHandleArbitraryOrdering() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.STRING)
        .build();
    final List<ColumnName> names = ImmutableList.of(COL0, KEY);

    // When:
    final KsqlGenericRecord record = recordFactory.build(
        names,
        ImmutableList.of(new StringLiteral("value"), new StringLiteral("key")),
        schema,
        DataSourceType.KSTREAM
    );

    // Then:
    assertThat(record, is(KsqlGenericRecord.of(
        GenericKey.genericKey("key"),
        GenericRow.genericRow("value"),
        0
    )));
  }

  @Test
  public void shouldBuildCoerceTypes() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.BIGINT)
        .valueColumn(COL0, SqlTypes.BIGINT)
        .build();
    final List<ColumnName> names = ImmutableList.of(KEY, COL0);
    final Expression exp = new IntegerLiteral(1);

    // When:
    final KsqlGenericRecord record = recordFactory.build(
        names, ImmutableList.of(exp, exp), schema, DataSourceType.KSTREAM
    );

    // Then:
    assertThat(record, is(KsqlGenericRecord.of(
        GenericKey.genericKey(1L),
        GenericRow.genericRow(1L),
        0
    )));
  }

  @Test
  public void shouldAcceptNullsForAnyColumn() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.BIGINT)
        .build();
    final List<ColumnName> names = ImmutableList.of(KEY, COL0);

    // When:
    final KsqlGenericRecord record = recordFactory.build(
        names, ImmutableList.of(new NullLiteral(), new NullLiteral()), schema, DataSourceType.KSTREAM
    );

    // Then:
    assertThat(record, is(KsqlGenericRecord.of(
        GenericKey.genericKey((Object) null),
        GenericRow.genericRow((Object) null),
        0
    )));
  }

  @Test
  public void shouldBuildWithRowtime() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.STRING)
        .build();
    final List<ColumnName> names = ImmutableList.of(SystemColumns.ROWTIME_NAME, KEY, COL0);
    final Expression exp = new StringLiteral("a");

    // When:
    final KsqlGenericRecord record = recordFactory.build(
        names, ImmutableList.of(new LongLiteral(1L), exp, exp), schema, DataSourceType.KSTREAM
    );

    // Then:
    assertThat(record, is(KsqlGenericRecord.of(
        GenericKey.genericKey("a"),
        GenericRow.genericRow("a"),
        1
    )));
  }

  @Test
  public void shouldUseClockTime() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.STRING)
        .build();
    final List<ColumnName> names = ImmutableList.of(KEY, COL0);
    final Expression exp = new StringLiteral("a");
    clock.set(1L);

    // When:
    final KsqlGenericRecord record = recordFactory.build(
        names, ImmutableList.of(exp, exp), schema, DataSourceType.KSTREAM
    );

    // Then:
    assertThat(record, is(KsqlGenericRecord.of(
        GenericKey.genericKey("a"),
        GenericRow.genericRow("a"),
        1
    )));
  }

  @Test
  public void shouldBuildPartialColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.STRING)
        .valueColumn(COL1, SqlTypes.STRING)
        .build();
    final List<ColumnName> names = ImmutableList.of(KEY, COL0);
    final Expression exp = new StringLiteral("a");

    // When:
    final KsqlGenericRecord record = recordFactory.build(
        names, ImmutableList.of(exp, exp), schema, DataSourceType.KSTREAM
    );

    // Then:
    assertThat(record, is(KsqlGenericRecord.of(
        GenericKey.genericKey("a"),
        GenericRow.genericRow("a", null),
        0
    )));
  }

  @Test
  public void shouldInferColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.STRING)
        .build();
    final List<ColumnName> names = ImmutableList.of();
    final Expression exp = new StringLiteral("a");

    // When:
    final KsqlGenericRecord record = recordFactory.build(
        names, ImmutableList.of(exp, exp), schema, DataSourceType.KSTREAM
    );

    // Then:
    assertThat(record, is(KsqlGenericRecord.of(
        GenericKey.genericKey("a"),
        GenericRow.genericRow("a"),
        0
    )));
  }

  @Test
  public void shouldThrowOnColumnMismatch() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.STRING)
        .valueColumn(COL1, SqlTypes.STRING)
        .build();
    final List<ColumnName> names = ImmutableList.of(KEY, COL0, COL1);
    final Expression exp = new StringLiteral("a");

    // When:
    final KsqlException e = assertThrows(KsqlException.class, () -> recordFactory.build(
        names, ImmutableList.of(exp, exp), schema, DataSourceType.KSTREAM
    ));

    // Then:
    assertThat(e.getMessage(), containsString("Expected a value for each column"));
  }

  @Test
  public void shouldThrowOnColumnMismatchWhenInferred() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.STRING)
        .valueColumn(COL1, SqlTypes.STRING)
        .build();
    final List<ColumnName> names = ImmutableList.of();
    final Expression exp = new StringLiteral("a");

    // When:
    final KsqlException e = assertThrows(KsqlException.class, () -> recordFactory.build(
        names, ImmutableList.of(exp, exp), schema, DataSourceType.KSTREAM
    ));

    // Then:
    assertThat(e.getMessage(), containsString("Expected a value for each column"));
  }

  @Test
  public void shouldThrowOnUnknownColumn() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.STRING)
        .build();

    final List<ColumnName> names = ImmutableList.of(KEY, COL1);
    final Expression exp = new StringLiteral("a");

    // When:
    final KsqlException e = assertThrows(KsqlException.class, () -> recordFactory.build(
        names, ImmutableList.of(exp, exp), schema, DataSourceType.KSTREAM
    ));

    // Then:
    assertThat(e.getMessage(), containsString("does not exist"));
  }

  @Test
  public void shouldThrowOnTableMissingKey() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.STRING)
        .valueColumn(COL1, SqlTypes.STRING)
        .build();
    final List<ColumnName> names = ImmutableList.of(COL0, COL1);
    final Expression exp = new StringLiteral("a");

    // When:
    final KsqlException e = assertThrows(KsqlException.class, () -> recordFactory.build(
        names, ImmutableList.of(exp, exp), schema, DataSourceType.KTABLE
    ));

    // Then:
    assertThat(e.getMessage(), containsString("Value for primary key column"));
  }

  @Test
  public void shouldThrowOnTypeMismatchCannotCoerce() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.INTEGER)
        .build();
    final List<ColumnName> names = ImmutableList.of(KEY, COL0);
    final Expression exp = new StringLiteral("a");

    // When:
    final KsqlException e = assertThrows(KsqlException.class, () -> recordFactory.build(
        names, ImmutableList.of(exp, exp), schema, DataSourceType.KSTREAM
    ));

    // Then:
    assertThat(e.getMessage(), containsString("Expected type"));
  }

  @Test
  public void shouldThrowOnInsertRowpartition() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.STRING)
        .build();
    final Expression exp = new StringLiteral("a");

    // When:
    final KsqlException e = assertThrows(KsqlException.class, () -> recordFactory.build(
        ImmutableList.of(SystemColumns.ROWTIME_NAME, KEY, SystemColumns.ROWPARTITION_NAME),
        ImmutableList.of(new LongLiteral(1L), exp, exp), schema, DataSourceType.KSTREAM
    ));

    // Then:
    assertThat(e.getMessage(), containsString("Inserting into column `ROWPARTITION` is not allowed."));
  }

  @Test
  public void shouldThrowOnInsertRowoffset() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(KEY, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.STRING)
        .build();
    final Expression exp = new StringLiteral("a");

    // When:
    final KsqlException e = assertThrows(KsqlException.class, () -> recordFactory.build(
        ImmutableList.of(SystemColumns.ROWTIME_NAME, KEY, SystemColumns.ROWOFFSET_NAME),
        ImmutableList.of(new LongLiteral(1L), exp, exp), schema, DataSourceType.KSTREAM
    ));

    // Then:
    assertThat(e.getMessage(), containsString("Inserting into column `ROWOFFSET` is not allowed."));
  }

}