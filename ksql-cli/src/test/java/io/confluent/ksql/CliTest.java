package io.confluent.ksql;

import io.confluent.ksql.cli.LocalCli;
import io.confluent.ksql.cli.console.OutputFormat;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CliTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final int PORT = 9098;
  private static final String SERVER = "http://localhost:" + PORT;
  private static final OutputFormat CLI_OUTPUT_FORMAT = OutputFormat.TABULAR;

  private static final long STREAMED_QUERY_ROW_LIMIT = 10000;
  private static final long STREAMED_QUERY_TIMEOUT_MS = 10000;

  private static LocalCli localCli;
  private static TestTerminal terminal;

  @BeforeClass
  public static void setUp() throws Exception {
    KsqlRestClient restClient = new KsqlRestClient(SERVER);

    // TODO: Fix Properties Setup in Local().getCli()
    // Local local =  new Local().getCli();
    // LocalCli localCli = local.getCli(restClient, terminal);

    // TODO: add remote cli test cases
    terminal = new TestTerminal(CLI_OUTPUT_FORMAT, restClient);

    localCli = new LocalCli(
        defaultServerProperties(),
        PORT,
        STREAMED_QUERY_ROW_LIMIT,
        STREAMED_QUERY_TIMEOUT_MS,
        restClient,
        terminal
    );
  }

  @AfterClass
  public static void tearDown() throws Exception {
    // If WARN NetworkClient:589 - Connection to node -1 could not be established. Broker may not be available.
    // It may be due to not closing the resource.
    // ksqlEngine.close();
    System.out.println("[Terminal Output]");
    System.out.println(terminal.getOutputString());

    localCli.close();
    terminal.close();
  }

  private static Map<String, Object> genDefaultConfigMap() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    configMap.put("application.id", "KSQL");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");
    configMap.put("command.topic.suffix", "commands");
    return configMap;
  }

  private static Properties defaultServerProperties() {
    Properties serverProperties = new Properties();
    serverProperties.putAll(genDefaultConfigMap());
    return serverProperties;
  }


  /* Only for coverage, not for validation */
  @Test
  public void testRunInteractively() {
    localCli.runInteractively();
  }

  @Test
  public void testEmptyInput() throws Exception {
    localCli.runNonInteractively("");
  }

  @Test
  public void testExitCommand() throws Exception {
    localCli.runNonInteractively("exit");
    localCli.runNonInteractively("\nexit\n\n\n");
    localCli.runNonInteractively("exit\nexit\nexit");
    localCli.runNonInteractively("\n\nexit\nexit\n\n\n\nexit\n\n\n");
  }

  @Test
  public void testExtraCommands() throws Exception {
    localCli.runNonInteractively("help");
    localCli.runNonInteractively("version");
    localCli.runNonInteractively("output");
    localCli.runNonInteractively("clear");
  }

}