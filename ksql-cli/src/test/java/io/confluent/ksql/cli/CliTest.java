package io.confluent.ksql.cli;

import io.confluent.ksql.rest.client.KSQLRestClient;
import org.junit.Test;

import javax.json.Json;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

// TODO: Verify that the expected output was printed to the console. Possibly through a package-private constructor that
// allows the terminal object to be injectable?
public class CliTest {

  private static Cli getTestCli(KSQLRestClient restClient) throws Exception {
    return new Cli(restClient, null, null);
  }

  private static Cli getTestCli() throws Exception {
    return getTestCli(null);
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
  public void testStatusInput() throws Exception {
    final String commandId = "topics/TEST_TOPIC";

    KSQLRestClient mockRestClient = mock(KSQLRestClient.class);
    expect(mockRestClient.makeStatusRequest()).andReturn(Json.createObjectBuilder().build());
    expect(mockRestClient.makeStatusRequest(commandId)).andReturn(Json.createObjectBuilder().build());
    replay(mockRestClient);

    getTestCli(mockRestClient).runNonInteractively(String.format("status\nstatus %s", commandId));
    verify(mockRestClient);
  }

  @Test
  public void testQueryInput() throws Exception {
    final String testBareQuery = "SELECT * FROM test_topic WHERE foo > bar;";

    KSQLRestClient.QueryStream mockQueryStream = mock(KSQLRestClient.QueryStream.class);
    // Not worth testing actual query rows being returned from the stream until output verification is possible
    expect(mockQueryStream.hasNext()).andReturn(false);
    mockQueryStream.close();
    expectLastCall();
    replay(mockQueryStream);

    KSQLRestClient mockRestClient = mock(KSQLRestClient.class);
    expect(mockRestClient.makeQueryRequest(testBareQuery)).andReturn(mockQueryStream);
    replay(mockRestClient);

    getTestCli(mockRestClient).runNonInteractively(testBareQuery);
    verify(mockRestClient);
    verify(mockQueryStream);
  }
}
