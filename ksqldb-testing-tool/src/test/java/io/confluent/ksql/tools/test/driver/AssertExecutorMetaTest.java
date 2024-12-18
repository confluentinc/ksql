package io.confluent.ksql.tools.test.driver;

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.hamcrest.Matchers;
import org.junit.Test;

/**
 * Most test coverage for the AssertExecutor is in the sql-tests/test.sql file
 * which is run through the KsqlTesterTest tool. This class is here to ensure
 * that the class evolves properly.
 */
public class AssertExecutorMetaTest {

  /**
   * These are excluded from coverage
   */
  private final Set<String> excluded = ImmutableSet.of(
      CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS,  // testing tool does not support partitions
      CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS,    // testing tool does not support replicas
      CommonCreateConfigs.SOURCE_TOPIC_RETENTION_IN_MS, // testing tool does not support retention_ms
      CommonCreateConfigs.SOURCE_TOPIC_CLEANUP_POLICY   // testing tool does not support cleanup_policy
  );

  @Test
  public void shouldCoverAllWithClauses() {
    // Given:
    final ConfigDef def = new ConfigDef();
    CommonCreateConfigs.addToConfigDef(def, true);
    final Set<String> allValidProps = def.names();

    // When:
    final Set<String> coverage = AssertExecutor.MUST_MATCH
        .stream()
        .map(sp -> sp.withClauseName)
        .flatMap(Arrays::stream)
        .collect(Collectors.toSet());

    // Then:
    assertThat(Sets.difference(allValidProps, coverage), Matchers.is(excluded));
  }

}