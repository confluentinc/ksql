package io.confluent.ksql.properties;

import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PropertyOverriderTest {

  public KsqlConfig ksqlConfig;
  public ConfiguredStatement<SetProperty> configuredStatement;

  @Mock
  public SetProperty setProperty;
  @Mock
  public KsqlParser.PreparedStatement<SetProperty> preparedStatement;

  @Before
  public void setUp() {
    ksqlConfig = new KsqlConfig(mkMap(mkEntry(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED, true)));
    configuredStatement = ConfiguredStatement.of(preparedStatement, SessionConfig.of(ksqlConfig, new HashMap<>()));
    when(preparedStatement.getStatement()).thenReturn(setProperty);
    when(setProperty.getPropertyName()).thenReturn("");
    when((setProperty.getPropertyValue())).thenReturn("");
  }

  @Test
  public void shouldNotSetNonQueryLevelParams()  {
    when(setProperty.getPropertyName()).thenReturn(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);
    when((setProperty.getPropertyValue())).thenReturn("100");
    Exception e = assertThrows(KsqlStatementException.class,
        () -> PropertyOverrider.set(configuredStatement, new HashMap<>()));
    assertThat(e.getMessage(), containsString("commit.interval.ms is not a settable property at the query"
        + " level with ksql.runtime.feature.shared.enabled on."
        + " Please use ALTER SYSTEM to set commit.interval.ms for the cluster.\n"
    ));
  }

  @Test
  public void shouldSetQueryLevelParams()  {
    when(setProperty.getPropertyName()).thenReturn(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
    when((setProperty.getPropertyValue())).thenReturn("100");
    PropertyOverrider.set(configuredStatement, new HashMap<>());
  }

}