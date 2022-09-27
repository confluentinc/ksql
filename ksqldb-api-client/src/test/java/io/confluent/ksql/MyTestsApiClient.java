package io.confluent.ksql;

import io.confluent.ksql.api.client.ClientBasicAuthTest;
import io.confluent.ksql.api.client.ClientTest;
import io.confluent.ksql.api.client.ClientTlsMutualAuthTest;
import io.confluent.ksql.api.client.ClientTlsTest;
import io.confluent.ksql.api.client.impl.ClientImplTest;
import io.confluent.ksql.api.client.impl.ClientOptionsImplTest;
import io.confluent.ksql.api.client.impl.ColumnTypeImplTest;
import io.confluent.ksql.api.client.impl.ConnectorDescriptionImplTest;
import io.confluent.ksql.api.client.impl.ConnectorInfoImplTest;
import io.confluent.ksql.api.client.impl.ConnectorTypeImplTest;
import io.confluent.ksql.api.client.impl.ExecuteStatementResultImplTest;
import io.confluent.ksql.api.client.impl.FieldInfoImplTest;
import io.confluent.ksql.api.client.impl.HttpRequestImplTest;
import io.confluent.ksql.api.client.impl.HttpResponseImplTest;
import io.confluent.ksql.api.client.impl.InsertAckImplTest;
import io.confluent.ksql.api.client.impl.PollableSubscriberTest;
import io.confluent.ksql.api.client.impl.QueryInfoImplTest;
import io.confluent.ksql.api.client.impl.RowImplTest;
import io.confluent.ksql.api.client.impl.ServerInfoImplTest;
import io.confluent.ksql.api.client.impl.SourceDescriptionImplTest;
import io.confluent.ksql.api.client.impl.StreamInfoImplTest;
import io.confluent.ksql.api.client.impl.StreamedQueryResultImplTest;
import io.confluent.ksql.api.client.impl.TableInfoImplTest;
import io.confluent.ksql.api.client.impl.TopicInfoImplTest;
import io.confluent.ksql.api.client.integration.AssertClientIntegrationTest;
import io.confluent.ksql.api.client.integration.ClientIntegrationTest;
import io.confluent.ksql.api.client.integration.ClientMutationIntegrationTest;
import io.confluent.ksql.api.client.integration.PushV2ClientContinueIntegrationTest;
import io.confluent.ksql.api.client.integration.RestClientIntegrationTest;
import io.confluent.ksql.api.client.integration.StreamStreamJoinsDeprecationNoticesIntegrationTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ClientImplTest.class,
        ClientOptionsImplTest.class,
        ColumnTypeImplTest.class,
        ConnectorDescriptionImplTest.class,
        ConnectorInfoImplTest.class,
        ConnectorTypeImplTest.class,
        ExecuteStatementResultImplTest.class,
        FieldInfoImplTest.class,
        HttpRequestImplTest.class,
        HttpResponseImplTest.class,
        InsertAckImplTest.class,
        PollableSubscriberTest.class,
        QueryInfoImplTest.class,
        RowImplTest.class,
        ServerInfoImplTest.class,
        SourceDescriptionImplTest.class,
        StreamedQueryResultImplTest.class,
        StreamInfoImplTest.class,
        TableInfoImplTest.class,
        TopicInfoImplTest.class,
//        Bazel doesn't support custom security managers, so these need to be re-worked.
//        AssertClientIntegrationTest.class,
//        ClientIntegrationTest.class,
//        ClientMutationIntegrationTest.class,
//        PushV2ClientContinueIntegrationTest.class,
//        RestClientIntegrationTest.class,
//        StreamStreamJoinsDeprecationNoticesIntegrationTest.class,
        ClientBasicAuthTest.class,
        ClientTest.class,
        ClientTlsMutualAuthTest.class,
        ClientTlsTest.class
})
public class MyTestsApiClient {
}
