package io.confluent.ksql.execution.codegen.helpers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.codegen.CodeGenTestUtil;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;

public class SqlTypeCodeGenTest {

  @Test
  public void shouldGenerateWorkingCodeForAllSqlBaseTypes() {
    for (final SqlBaseType baseType : SqlBaseType.values()) {

      // When:
      final String code = SqlTypeCodeGen.generateCode(TypeInstances.typeInstanceFor(baseType));

      // Then:
      final Object result = CodeGenTestUtil.cookAndEval(code, SqlType.class);
      assertThat(result, is(instanceOf(SqlType.class)));
      assertThat(((SqlType) result).baseType(), is(baseType));
    }
  }

  private static final class TypeInstances {

    private static final ImmutableMap<SqlBaseType, SqlType> TYPE_INSTANCES = ImmutableMap.
        <SqlBaseType, SqlType>builder()
        .put(SqlBaseType.BOOLEAN, SqlTypes.BOOLEAN)
        .put(SqlBaseType.INTEGER, SqlTypes.INTEGER)
        .put(SqlBaseType.BIGINT, SqlTypes.BIGINT)
        .put(SqlBaseType.DECIMAL, SqlTypes.decimal(4, 2))
        .put(SqlBaseType.DOUBLE, SqlTypes.DOUBLE)
        .put(SqlBaseType.STRING, SqlTypes.STRING)
        .put(SqlBaseType.TIMESTAMP, SqlTypes.TIMESTAMP)
        .put(SqlBaseType.ARRAY, SqlTypes.array(SqlTypes.BIGINT))
        .put(SqlBaseType.MAP, SqlTypes.map(SqlTypes.BIGINT, SqlTypes.STRING))
        .put(SqlBaseType.STRUCT, SqlTypes.struct()
            .field("Bob", SqlTypes.STRING)
            .build())
        .build();

    static SqlType typeInstanceFor(final SqlBaseType baseType) {
      final SqlType sqlType = TYPE_INSTANCES.get(baseType);
      assertThat(
          "Invalid test: missing type instance for " + baseType,
          sqlType,
          is(notNullValue())
      );
      return sqlType;
    }
  }
}