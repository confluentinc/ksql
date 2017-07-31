package io.confluent.ksql.cli;

import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.cli.console.JLineTerminal;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

// TODO: Verify that the expected output was printed to the console. Possibly through a package-private constructor that
// allows the terminal object to be injectable?
public class CliTest {

  private static Cli getTestCli(KsqlRestClient restClient) throws Exception {
    Console terminal = new JLineTerminal(Cli.OutputFormat.JSON, restClient);
    return new Cli(null, null, restClient, terminal);
  }

  private static Cli getTestCli() throws Exception {
    return getTestCli(mock(KsqlRestClient.class));
  }

  @Test
  public void testEmptyInput() throws Exception {
    getTestCli().runNonInteractively("");
  }

  @Test
  public void testExitInput() throws Exception {
    getTestCli().runNonInteractively("exit");
    getTestCli().runNonInteractively("\nexit\n\n\n");
    getTestCli().runNonInteractively("exit\nexit\nexit");
    getTestCli().runNonInteractively("\n\nexit\nexit\n\n\n\nexit\n\n\n");
  }

  @Test
  public void testHelpInput() throws Exception {
    getTestCli().runNonInteractively("help");
  }

  @Test
  public void testQueryInput() throws Exception {
    final String testBareQuery = "SELECT * FROM test_topic WHERE foo > bar;";

    KsqlRestClient.QueryStream mockQueryStream = mock(KsqlRestClient.QueryStream.class);
    // Not worth testing actual query rows being returned from the stream until output verification is possible
    expect(mockQueryStream.hasNext()).andReturn(false);
    mockQueryStream.close();
    expectLastCall();
    replay(mockQueryStream);

    KsqlRestClient mockRestClient = mock(KsqlRestClient.class);
    expect(mockRestClient.makeQueryRequest(testBareQuery))
        .andReturn(RestResponse.successful(mockQueryStream));
    replay(mockRestClient);

    getTestCli(mockRestClient).runNonInteractively(testBareQuery);
    verify(mockRestClient);
    verify(mockQueryStream);
  }
}