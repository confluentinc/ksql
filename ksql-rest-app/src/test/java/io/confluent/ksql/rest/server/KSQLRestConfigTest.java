package io.confluent.ksql.rest.server;

import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;

public class KSQLRestConfigTest {

  private Map<String, Object> getBaseProperties() {
    Map<String, Object> result = new HashMap<>();
    result.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    result.put(StreamsConfig.APPLICATION_ID_CONFIG, "ksql_config_test");
    result.put(KSQLRestConfig.COMMAND_TOPIC_SUFFIX_CONFIG, "commands");
    return result;
  }

  private void assertKeyEquals(String key, Map<String, ?> expected, Map<String, ?> test) {
    assertEquals(expected.get(key), test.get(key));
  }

  @Test
  public void testGetKsqlStreamsProperties() {
    final long BASE_COMMIT_INTERVAL_MS = 1000;
    final long OVERRIDE_COMMIT_INTERVAL_MS = 100;

    final String OVERRIDE_BOOTSTRAP_SERVERS = "ksql.io.confluent:6969";

    assertNotEquals(BASE_COMMIT_INTERVAL_MS, OVERRIDE_COMMIT_INTERVAL_MS);

    Map<String, Object> inputProperties = getBaseProperties();
    inputProperties.put(
        StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
        BASE_COMMIT_INTERVAL_MS
    );
    inputProperties.put(
        KSQLRestConfig.KSQL_STREAMS_PREFIX + StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
        OVERRIDE_COMMIT_INTERVAL_MS
    );
    inputProperties.put(
        KSQLRestConfig.KSQL_STREAMS_PREFIX + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        OVERRIDE_BOOTSTRAP_SERVERS
    );

    Map<String, Object> testProperties = new KSQLRestConfig(inputProperties).getKsqlStreamsProperties();

    assertEquals(
        OVERRIDE_COMMIT_INTERVAL_MS,
        testProperties.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG)
    );
    assertEquals(
        OVERRIDE_BOOTSTRAP_SERVERS,
        testProperties.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)
    );
  }

  // Just a sanity check to make sure that, although they contain identical mappings, successive maps returned by calls
  // to KSQLRestConfig.getOriginals() do not actually return the same object (mutability would then be an issue)
  @Test
  public void testOriginalsReplicability() {
    final String COMMIT_INTERVAL_MS = "10";

    Map<String, Object> inputProperties = getBaseProperties();
    inputProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
    KSQLRestConfig config = new KSQLRestConfig(inputProperties);

    final Map<String, Object> originals1 = config.getOriginals();
    final Map<String, Object> originals2 = config.getOriginals();

    assertEquals(originals1, originals2);
    assertNotSame(originals1, originals2);
    assertEquals(COMMIT_INTERVAL_MS, originals1.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));
    assertEquals(COMMIT_INTERVAL_MS, originals2.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));
  }
}
