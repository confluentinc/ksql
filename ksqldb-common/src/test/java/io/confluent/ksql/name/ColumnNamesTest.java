package io.confluent.ksql.name;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.name.ColumnNames.AliasGenerator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Test;

public class ColumnNamesTest {

  @Test
  public void shouldStartGeneratingFromZeroIfSourceSchemasHaveNoGeneratedAliases() {
    // Given:
    final Supplier<ColumnName> generator = ColumnNames
        .columnAliasGenerator(Stream.of(LogicalSchema.builder().build()));

    // When:
    final ColumnName result = generator.get();

    // Then:
    assertThat(result, is(ColumnName.of("KSQL_COL_0")));
  }

  @Test
  public void shouldAvoidClashesWithSourceColumnNames() {
    // Given:
    final Supplier<ColumnName> generator = ColumnNames
        .columnAliasGenerator(Stream.of(LogicalSchema.builder()
            .keyColumn(ColumnName.of("Fred"), SqlTypes.STRING)
            .keyColumn(ColumnName.of("KSQL_COL_3"), SqlTypes.STRING)
            .keyColumn(ColumnName.of("KSQL_COL_1"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("KSQL_COL_1"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("George"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("KSQL_COL_5"), SqlTypes.STRING)
            .build()
        ));

    // When:
    final List<ColumnName> result = IntStream.range(0, 5)
        .mapToObj(idx -> generator.get())
        .collect(Collectors.toList());

    // Then:
    assertThat(result, contains(
        ColumnName.of("KSQL_COL_0"),
        ColumnName.of("KSQL_COL_2"),
        ColumnName.of("KSQL_COL_4"),
        ColumnName.of("KSQL_COL_6"),
        ColumnName.of("KSQL_COL_7")
    ));
  }

  @Test
  public void shouldThrowIfIndexOverflows() {
    // Given:
    final AliasGenerator generator =
        new AliasGenerator(Integer.MAX_VALUE, ImmutableSet.of());

    generator.next(); // returns MAX_VALUE.

    // When:
    assertThrows(KsqlException.class, generator::next);
  }
}