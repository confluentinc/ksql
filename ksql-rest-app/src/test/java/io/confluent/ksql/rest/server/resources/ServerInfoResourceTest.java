package io.confluent.ksql.rest.server.resources;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Version;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.core.Response;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class ServerInfoResourceTest {
  private static final String KSQL_SERVICE_ID = "ksql.foo";
  private static final String KAFKA_CLUSTER_ID = "kafka.bar";

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private AdminClient adminClient;
  @Mock
  private DescribeClusterResult describeClusterResult;
  @Mock
  private KafkaFuture<String> future;

  private ServerInfoResource serverInfoResource = null;

  private final KsqlConfig ksqlConfig = new KsqlConfig(
      ImmutableMap.of(
        KsqlConfig.KSQL_SERVICE_ID_CONFIG, KSQL_SERVICE_ID
      )
  );

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() throws InterruptedException, TimeoutException, ExecutionException {
    when(serviceContext.getAdminClient()).thenReturn(adminClient);
    when(adminClient.describeCluster()).thenReturn(describeClusterResult);
    when(describeClusterResult.clusterId()).thenReturn(future);
    when(future.get(anyLong(), any())).thenReturn(KAFKA_CLUSTER_ID);

    serverInfoResource = new ServerInfoResource(serviceContext, ksqlConfig);
  }

  @Test
  public void shouldReturnServerInfo() {
    // When:
    final Response response = serverInfoResource.get();

    // Then:
    assertThat(response.getStatus(), equalTo(200));
    assertThat(response.getEntity(), instanceOf(ServerInfo.class));
    final ServerInfo serverInfo = (ServerInfo)response.getEntity();
    assertThat(
        serverInfo,
        equalTo(new ServerInfo(Version.getVersion(), KAFKA_CLUSTER_ID, KSQL_SERVICE_ID))
    );
  }

  @Test
  public void shouldGetKafkaClusterIdWithTimeout()
      throws InterruptedException, ExecutionException, TimeoutException{
    // When:
    serverInfoResource.get();

    // Then:
    verify(future).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void shouldGetKafkaClusterIdOnce() {
    // When:
    serverInfoResource.get();
    serverInfoResource.get();

    // Then:
    verify(adminClient).describeCluster();
    verifyNoMoreInteractions(adminClient);
  }
}