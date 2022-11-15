package io.confluent.ksql.parser.json;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.types.SqlTypes;

public class ColumnTestCase {
  static final Column COLUMN = Column.of(
      ColumnName.of("FOO"),
      SqlTypes.INTEGER,
      Namespace.VALUE,
      0
  );

  static final String COLUMN_STRING = "\"`FOO` INTEGER\"";
}
