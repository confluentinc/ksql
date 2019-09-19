package io.confluent.ksql.execution.testutil;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
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
      .valueColumn(ColumnName.of("TEST1.COL0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("TEST1.COL1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("TEST1.COL2"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("TEST1.COL3"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("TEST1.COL4"), SqlTypes.array(SqlTypes.DOUBLE))
      .valueColumn(ColumnName.of("TEST1.COL5"), SqlTypes.map(SqlTypes.DOUBLE))
      .valueColumn(ColumnName.of("TEST1.COL6"), ADDRESS_SCHEMA)
      .valueColumn(ColumnName.of("TEST1.COL7"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("TEST1.COL8"), SqlTypes.decimal(2, 1))
      .valueColumn(ColumnName.of("TEST1.COL9"), SqlTypes.decimal(2, 1))
      .build();

  private static final String TEST1 = "TEST1";
  public static final ColumnReferenceExp COL0 = columnRef(TEST1, "COL0");
  public static final ColumnReferenceExp COL1 = columnRef(TEST1, "COL1");
  public static final ColumnReferenceExp COL2 = columnRef(TEST1, "COL2");
  public static final ColumnReferenceExp COL3 = columnRef(TEST1, "COL3");
  public static final ColumnReferenceExp ADDRESS = columnRef(TEST1, "COL6");
  public static final ColumnReferenceExp ARRAYCOL = columnRef(TEST1, "COL4");
  public static final ColumnReferenceExp MAPCOL = columnRef(TEST1, "COL5");
  public static final ColumnReferenceExp COL7 = columnRef(TEST1, "COL7");

  private static ColumnReferenceExp columnRef(final String source, final String name) {
    return new ColumnReferenceExp(ColumnRef.of(SourceName.of(source), ColumnName.of(name)));
  }

  public static Expression literal(int value) {
    return new IntegerLiteral(value);
  }
}
