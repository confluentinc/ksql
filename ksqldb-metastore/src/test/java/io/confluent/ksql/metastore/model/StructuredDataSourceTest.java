/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.metastore.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StructuredDataSourceTest {

  @Mock
  private KsqlTopic topic;
  @Mock
  private KsqlTopic topic2;

  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
      .build();

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSchemaContainsRowTime() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .valueColumn(SystemColumns.ROWTIME_NAME, SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
        .build();

    // When:
    new TestStructuredDataSource(schema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnDuplicateColumnNames() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("dup"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("dup"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
        .build();

    // When:
    new TestStructuredDataSource(schema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSchemaContainsWindowStart() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .valueColumn(SystemColumns.WINDOWSTART_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
        .build();

    // When:
    new TestStructuredDataSource(schema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSchemaContainsWindowEnd() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .valueColumn(SystemColumns.WINDOWEND_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
        .build();

    // When:
    new TestStructuredDataSource(schema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSchemaContainsValueColumnsWithSameNameAsKeyColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .valueColumn(SystemColumns.WINDOWEND_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("k0"), SqlTypes.BIGINT)
        .build();

    // When:
    new TestStructuredDataSource(schema);
  }

  @Test
  public void shouldEnforceSameNameCompatbility() {
    // Given:
    final KsqlStream<String> streamA = new KsqlStream<>(
        "sql",
        SourceName.of("A"),
        SOME_SCHEMA,
        SerdeOptions.of(),
        Optional.empty(),
        true,
        topic
    );
    final KsqlStream<String> streamB = new KsqlStream<>(
        "sql",
        SourceName.of("B"),
        SOME_SCHEMA,
        SerdeOptions.of(),
        Optional.empty(),
        true,
        topic
    );

    // When:
    final Optional<String> err = streamA.canUpgradeTo(streamB);

    // Then:
    assertThat(err.isPresent(), is(true));
    assertThat(err.get(), containsString("has name = `A` which is not upgradeable to `B`"));
  }

  @Test
  public void shouldEnforceSameSerdeOptions() {
    // Given:
    final KsqlStream<String> streamA = new KsqlStream<>(
        "sql",
        SourceName.of("A"),
        SOME_SCHEMA,
        SerdeOptions.of(),
        Optional.empty(),
        true,
        topic
    );
    final KsqlStream<String> streamB = new KsqlStream<>(
        "sql",
        SourceName.of("A"),
        SOME_SCHEMA,
        SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES),
        Optional.empty(),
        true,
        topic
    );

    // When:
    final Optional<String> err = streamA.canUpgradeTo(streamB);

    // Then:
    assertThat(err.isPresent(), is(true));
    assertThat(err.get(), containsString("has serdeOptions = SerdeOptions[] which is not upgradeable to SerdeOptions[UNWRAP_SINGLE_VALUES]"));
  }

  @Test
  public void shouldEnforceSameTimestampColumn() {
    // Given:
    final KsqlStream<String> streamA = new KsqlStream<>(
        "sql",
        SourceName.of("A"),
        SOME_SCHEMA,
        SerdeOptions.of(),
        Optional.empty(),
        true,
        topic
    );
    final KsqlStream<String> streamB = new KsqlStream<>(
        "sql",
        SourceName.of("A"),
        SOME_SCHEMA,
        SerdeOptions.of(),
        Optional.of(new TimestampColumn(ColumnName.of("foo"), Optional.empty())),
        true,
        topic
    );

    // When:
    final Optional<String> err = streamA.canUpgradeTo(streamB);

    // Then:
    assertThat(err.isPresent(), is(true));
    assertThat(err.get(), containsString("has timestampColumn = Optional.empty which is not upgradeable to Optional[TimestampColumn{column=`foo`, format=Optional.empty}]"));
  }

  @Test
  public void shouldEnforceSameTopic() {
    // Given:
    final KsqlStream<String> streamA = new KsqlStream<>(
        "sql",
        SourceName.of("A"),
        SOME_SCHEMA,
        SerdeOptions.of(),
        Optional.empty(),
        true,
        topic
    );
    final KsqlStream<String> streamB = new KsqlStream<>(
        "sql",
        SourceName.of("A"),
        SOME_SCHEMA,
        SerdeOptions.of(),
        Optional.empty(),
        true,
        topic2
    );

    // When:
    final Optional<String> err = streamA.canUpgradeTo(streamB);

    // Then:
    assertThat(err.isPresent(), is(true));
    assertThat(err.get(), containsString("has topic = topic which is not upgradeable to topic2"));
  }

  @Test
  public void shouldEnforceSameType() {
    // Given:
    final KsqlStream<String> streamA = new KsqlStream<>(
        "sql",
        SourceName.of("A"),
        SOME_SCHEMA,
        SerdeOptions.of(),
        Optional.empty(),
        true,
        topic
    );
    final KsqlTable<String> streamB = new KsqlTable<>(
        "sql",
        SourceName.of("A"),
        SOME_SCHEMA,
        SerdeOptions.of(),
        Optional.empty(),
        true,
        topic
    );

    // When:
    final Optional<String> err = streamA.canUpgradeTo(streamB);

    // Then:
    assertThat(err.isPresent(), is(true));
    assertThat(err.get(), containsString("has type = KSTREAM which is not upgradeable to KTABLE"));
  }

  @Test
  public void shouldEnforceNoRemovedColumns() {
    // Given:
    final LogicalSchema someSchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("f1"), SqlTypes.BIGINT)
        .build();

    final LogicalSchema otherSchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
        .build();

    // When:
    final Optional<String> s = StructuredDataSource.checkSchemas(someSchema, otherSchema);

    // Then:
    assertThat(s.isPresent(), is(true));
    assertThat(s.get(), containsString("The following columns are changed or missing: [`f1` BIGINT]"));
  }

  @Test
  public void shouldEnforceNoTypeChange() {
    // Given:
    final LogicalSchema someSchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
        .build();

    final LogicalSchema otherSchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("f0"), SqlTypes.STRING)
        .build();

    // When:
    final Optional<String> s = StructuredDataSource.checkSchemas(someSchema, otherSchema);

    // Then:
    assertThat(s.isPresent(), is(true));
    assertThat(s.get(), containsString("The following columns are changed or missing: [`f0` BIGINT]"));
  }

  /**
   * Test class to allow the abstract base class to be instantiated.
   */
  private final class TestStructuredDataSource extends StructuredDataSource<String> {

    private TestStructuredDataSource(
        final LogicalSchema schema
    ) {
      super(
          "some SQL",
          SourceName.of("some name"),
          schema,
          SerdeOptions.of(),
          Optional.empty(),
          DataSourceType.KSTREAM,
          false,
          topic
      );
    }
  }
}