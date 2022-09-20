package io.confluent.ksql;

import io.confluent.ksql.rocksdb.KsqlBoundedMemoryRocksDBConfigSetterTest;
import io.confluent.ksql.rocksdb.KsqlBoundedMemoryRocksDBConfigTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        KsqlBoundedMemoryRocksDBConfigTest.class,
        KsqlBoundedMemoryRocksDBConfigSetterTest.class
})
public class MyTestsRocksDbConfigSetter {
}
