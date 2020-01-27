package io.confluent.ksql.execution.testutil;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;

public final class TestExpressions {

  private TestExpressions() {
  }

  private final static SqlStruct ADDRESS_SCHEMA = SqlTypes.struct()
      .field("NUMBER", SqlTypes.BIGINT)
      .field("STREET", SqlTypes.STRING)
      .field("CITY", SqlTypes.STRING)
      .field("STATE", SqlTypes.STRING)
      .field("ZIPCODE", SqlTypes.BIGINT)
      .build();

  public final static LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("COL1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("COL2"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("COL3"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("COL4"), SqlTypes.array(SqlTypes.DOUBLE))
      .valueColumn(ColumnName.of("COL5"), SqlTypes.map(SqlTypes.DOUBLE))
      .valueColumn(ColumnName.of("COL6"), ADDRESS_SCHEMA)
      .valueColumn(ColumnName.of("COL7"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("COL8"), SqlTypes.decimal(2, 1))
      .valueColumn(ColumnName.of("COL9"), SqlTypes.decimal(2, 1))
      .build();

  public static final UnqualifiedColumnReferenceExp COL0 = columnRef("COL0");
  public static final UnqualifiedColumnReferenceExp COL1 = columnRef("COL1");
  public static final UnqualifiedColumnReferenceExp COL2 = columnRef("COL2");
  public static final UnqualifiedColumnReferenceExp COL3 = columnRef("COL3");
  public static final UnqualifiedColumnReferenceExp ADDRESS = columnRef("COL6");
  public static final UnqualifiedColumnReferenceExp ARRAYCOL = columnRef("COL4");
  public static final UnqualifiedColumnReferenceExp MAPCOL = columnRef("COL5");
  public static final UnqualifiedColumnReferenceExp COL7 = columnRef("COL7");

  private static UnqualifiedColumnReferenceExp columnRef(final String name) {
    return new UnqualifiedColumnReferenceExp(ColumnRef.of(ColumnName.of(name)));
  }

  public static Expression literal(final int value) {
    return new IntegerLiteral(value);
  }
}
