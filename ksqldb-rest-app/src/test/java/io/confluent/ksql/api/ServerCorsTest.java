package io.confluent.ksql.api;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.state.ServerState;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class ServerCorsTest extends BaseApiTest {
  private final CorsTest corsTest = new CorsTest(this::init);

  @Before
  public void setUp() {

    vertx = Vertx.vertx();
    vertx.exceptionHandler(t -> log.error("Unhandled exception in Vert.x", t));

    testEndpoints = new TestEndpoints();
    serverState = new ServerState();
    serverState.setReady();
    setDefaultRowGenerator();
  }

  @Test
  public void shouldNotBeCorsResponseIfNoCorsConfigured() throws Exception {
    corsTest.shouldNotBeCorsResponseIfNoCorsConfigured();
  }

  @Test
  public void shouldExcludePath() throws Exception {
    corsTest.shouldExcludePath(404);
  }

  @Test
  public void shouldNotBeCorsResponseIfNotCorsRequest() throws Exception {
    corsTest.shouldNotBeCorsResponseIfNotCorsRequest();
  }

  @Test
  public void shouldAcceptCorsRequestOriginExactMatch() throws Exception {
    corsTest.shouldAcceptCorsRequestOriginExactMatch();
  }

  @Test
  public void shouldRejectCorsRequestOriginExactMatch() throws Exception {
    corsTest.shouldRejectCorsRequestOriginExactMatch();
  }

  @Test
  public void shouldAcceptCorsRequestOriginExactMatchOneOfList() throws Exception {
    corsTest.shouldAcceptCorsRequestOriginExactMatchOneOfList();
  }

  @Test
  public void shouldRejectCorsRequestOriginExactMatchOenOfList() throws Exception {
    corsTest.shouldRejectCorsRequestOriginExactMatchOenOfList();
  }

  @Test
  public void shouldAcceptCorsRequestAcceptAll() throws Exception {
    corsTest.shouldAcceptCorsRequestAcceptAll();
  }

  @Test
  public void shouldAcceptCorsRequestWildcardMatch() throws Exception {
    corsTest.shouldAcceptCorsRequestWildcardMatch();
  }

  @Test
  public void shouldRejectCorsRequestWildcardMatch() throws Exception {
    corsTest.shouldRejectCorsRequestWildcardMatch();
  }

  @Test
  public void shouldAcceptCorsRequestWilcardMatchList() throws Exception {
    corsTest.shouldAcceptCorsRequestWilcardMatchList();
  }

  @Test
  public void shouldRejectCorsRequestWilcardMatchList() throws Exception {
    corsTest.shouldRejectCorsRequestWilcardMatchList();
  }

  @Test
  public void shouldAcceptPreflightRequestOriginExactMatchDefaultHeaders() throws Exception {
    corsTest.shouldAcceptPreflightRequestOriginExactMatchDefaultHeaders();
  }

  @Test
  public void shouldRejectPreflightRequestOriginExactMatchDefaultHeaders() throws Exception {
    corsTest.shouldRejectPreflightRequestOriginExactMatchDefaultHeaders();
  }

  @Test
  public void shouldAcceptPreflightRequestOriginExactMatchSpecifiedHeaders() throws Exception {
    corsTest.shouldAcceptPreflightRequestOriginExactMatchSpecifiedHeaders();
  }

  @Test
  public void shouldAcceptPreflightRequestOriginExactMatchSpecifiedMethods() throws Exception {
    corsTest.shouldAcceptPreflightRequestOriginExactMatchSpecifiedMethods();
  }

  @Test
  public void shouldCallAllCorsTests() {
    corsTest.shouldCallAllCorsTests(this.getClass());
  }

  private WebClient init(final Map<String, String> configs) {
    KsqlRestConfig serverConfig = new KsqlRestConfig(
        ImmutableMap.<String, Object>builder()
            .putAll(configs)
            .put(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0").build()
    );
    createServer(serverConfig);
    this.client = createClient();
    return client;
  }
}

