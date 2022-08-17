package io.confluent.ksql.statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateConnector;
import org.junit.Test;

public class ConfiguredStatementTest {

  @Test
  public void shouldMaskConfiguredStatement() {

    // Given
    final String query = "--this is a comment. \n"
        + "CREATE SOURCE CONNECTOR `test-connector` WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url' = 'jdbc:postgresql://localhost:5432/my.db',\n"
        + "    \"mode\"='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";

    final String masked = "CREATE SOURCE CONNECTOR `test-connector` WITH "
        + "(\"connector.class\"='PostgresSource', "
        + "'connection.url'='[string]', "
        + "\"mode\"='[string]', "
        + "\"topic.prefix\"='[string]', "
        + "\"table.whitelist\"='[string]', "
        + "\"key\"='[string]');";

    final String maskedToString = "ConfiguredStatement{"
        + "statement=" + masked
        + ", config=";

    final PreparedStatement<CreateConnector> preparedStatement =
        PreparedStatement.of(query, mock(CreateConnector.class));
    // when
    final ConfiguredStatement<CreateConnector> configuredStatement =
        ConfiguredStatement.of(preparedStatement, mock(SessionConfig.class));

    // Then
    assertThat(configuredStatement.getMaskedStatementText(), is(masked));
    assertThat(configuredStatement.getUnMaskedStatementText(), is(query));
    assertThat(configuredStatement.toString(), containsString(maskedToString));
  }
}
