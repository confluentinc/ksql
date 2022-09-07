package io.confluent.ksql;

import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercerTest;
import io.confluent.ksql.schema.ksql.SqlBooleansTest;
import io.confluent.ksql.schema.ksql.SqlDoublesTest;
import io.confluent.ksql.schema.ksql.SqlTimeTypesTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        DefaultSqlValueCoercerTest.class,
        SqlBooleansTest.class,
        SqlDoublesTest.class,
        SqlTimeTypesTest.class,
})
public class MyTestsEngineCommon {
}
