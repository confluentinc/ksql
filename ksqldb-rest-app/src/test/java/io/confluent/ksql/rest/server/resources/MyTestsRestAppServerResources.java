package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.rest.server.resources.streaming.PollingSubscriptionTest;
import io.confluent.ksql.rest.server.resources.streaming.PrintSubscriptionTest;
import io.confluent.ksql.rest.server.resources.streaming.PullQueryPublisherTest;
import io.confluent.ksql.rest.server.resources.streaming.PullQueryStreamWriterTest;
import io.confluent.ksql.rest.server.resources.streaming.QueryStreamWriterTest;
import io.confluent.ksql.rest.server.resources.streaming.RecordFormatterTest;
import io.confluent.ksql.rest.server.resources.streaming.SessionUtilTest;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResourceTest;
import io.confluent.ksql.rest.server.resources.streaming.TombstoneFactoryTest;
import io.confluent.ksql.rest.server.resources.streaming.TopicStreamWriterTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        PollingSubscriptionTest.class,
        PrintSubscriptionTest.class,
        PullQueryPublisherTest.class,
        PullQueryStreamWriterTest.class,
        QueryStreamWriterTest.class,
        RecordFormatterTest.class,
        SessionUtilTest.class,
        StreamedQueryResourceTest.class,
        TombstoneFactoryTest.class,
        TopicStreamWriterTest.class,
        ClusterStatusResourceTest.class,
        HealthCheckResourceTest.class,
        HeartbeatResourceTest.class,
//        KsqlResourceTest.class,
        ServerInfoResourceTest.class,
        ServerMetadataResourceTest.class,
        StatusResourceTest.class,
        WSQueryEndpointTest.class,
})
public class MyTestsRestAppServerResources {
}
