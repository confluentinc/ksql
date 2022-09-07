package io.confluent.ksql;

import io.confluent.ksql.rest.ApiJsonMapperTest;
import io.confluent.ksql.rest.ErrorsTest;
import io.confluent.ksql.rest.GenericRowSerializationTest;
import io.confluent.ksql.rest.QueryIdSerializationTest;
import io.confluent.ksql.rest.entity.CommandStatusEntityTest;
import io.confluent.ksql.rest.entity.EntityTest;
import io.confluent.ksql.rest.entity.FunctionInfoTest;
import io.confluent.ksql.rest.entity.KafkaTopicsListExtendedTest;
import io.confluent.ksql.rest.entity.KafkaTopicsListTest;
import io.confluent.ksql.rest.entity.KsqlErrorMessageTest;
import io.confluent.ksql.rest.entity.KsqlMediaTypeTest;
import io.confluent.ksql.rest.entity.KsqlRequestTest;
import io.confluent.ksql.rest.entity.QueryStatusCountTest;
import io.confluent.ksql.rest.entity.SchemaDescriptionFormatTest;
import io.confluent.ksql.rest.entity.SchemaInfoTest;
import io.confluent.ksql.rest.entity.ServerClusterIdTest;
import io.confluent.ksql.rest.entity.ServerInfoTest;
import io.confluent.ksql.rest.entity.ServerMetadataTest;
import io.confluent.ksql.rest.entity.SourceDescriptionTest;
import io.confluent.ksql.rest.entity.SourceInfoTest;
import io.confluent.ksql.rest.model.ImmutabilityTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        CommandStatusEntityTest.class,
        EntityTest.class,
        FunctionInfoTest.class,
        KafkaTopicsListExtendedTest.class,
        KafkaTopicsListTest.class,
        KafkaTopicsListTest.class,
        KsqlErrorMessageTest.class,
        KsqlMediaTypeTest.class,
        KsqlRequestTest.class,
        QueryStatusCountTest.class,
        SchemaDescriptionFormatTest.class,
        SchemaInfoTest.class,
        ServerClusterIdTest.class,
        ServerInfoTest.class,
        ServerMetadataTest.class,
        SourceDescriptionTest.class,
        SourceInfoTest.class,
        ImmutabilityTest.class,
        ApiJsonMapperTest.class,
        ErrorsTest.class,
        GenericRowSerializationTest.class,
        QueryIdSerializationTest.class,
})
public class MyTestsRestModel {
}
