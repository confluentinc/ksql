package io.confluent.ksql;

import io.confluent.ksql.function.udaf.VariadicArgsTest;
import io.confluent.ksql.schema.ksql.SqlArgumentTest;
import io.confluent.ksql.schema.ksql.types.SqlArrayTest;
import io.confluent.ksql.schema.ksql.types.SqlDecimalTest;
import io.confluent.ksql.schema.ksql.types.SqlMapTest;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveTypeTest;
import io.confluent.ksql.schema.ksql.types.SqlStructTest;
import io.confluent.ksql.schema.ksql.types.SqlTypesTest;
import io.confluent.ksql.types.KsqlStructTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        VariadicArgsTest.class,
        SqlArrayTest.class,
        SqlDecimalTest.class,
        SqlMapTest.class,
        SqlPrimitiveTypeTest.class,
        SqlStructTest.class,
        SqlTypesTest.class,
        SqlArgumentTest.class,
        KsqlStructTest.class,
})
public class MyTestsUdf {
}