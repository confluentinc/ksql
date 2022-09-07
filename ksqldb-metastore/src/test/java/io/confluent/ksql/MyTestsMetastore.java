package io.confluent.ksql;

import io.confluent.ksql.metastore.ImmutabilityTest;
import io.confluent.ksql.metastore.MetaStoreImplTest;
import io.confluent.ksql.metastore.model.MetaStoreModelTest;
import io.confluent.ksql.metastore.model.StructuredDataSourceTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        MetaStoreModelTest.class,
        StructuredDataSourceTest.class,
        ImmutabilityTest.class,
        MetaStoreImplTest.class,
})
public class MyTestsMetastore {
}
