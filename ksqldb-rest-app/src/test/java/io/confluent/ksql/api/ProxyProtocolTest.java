/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;

import io.confluent.ksql.rest.server.KsqlRestConfig;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class ProxyProtocolTest extends ApiTest {

  @Override
  protected KsqlRestConfig createServerConfig() {
    final Map<String, Object> config = new HashMap<>();
    config.put(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8080,http://localhost:8081");
    config.put(KsqlRestConfig.PROXY_PROTOCOL_LISTENERS_CONFIG, "http://localhost:8081");
    config.put(KsqlRestConfig.VERTICLE_INSTANCES, 4);
    return new KsqlRestConfig(config);
  }

  @Test
  public void shouldHandleRequestsOnProxyProtocolListener() throws Exception {
    final String host = server.getProxyProtocolListeners().get(0).getHost();
    final int port = server.getProxyProtocolListeners().get(0).getPort();

    Socket clientSocket = new Socket(host, port);
    BufferedWriter out = new BufferedWriter(
        new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8)
    );
    BufferedReader in = new BufferedReader(
        new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8)
    );
    // See https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
    out.write("PROXY TCP4 198.51.100.22 127.0.0.1 4334 " + port + "\r\n");
    out.write("GET /info HTTP/1.0\r\n");
    out.write("Host: " + host + "\r\n");
    out.write("\r\n");
    out.flush();
    String s = in.readLine();
    assertThat(s, startsWith("HTTP/1.0 200 OK"));
    out.close();
    in.close();
    clientSocket.close();
  }

  @Test
  public void shouldNotHandleRequestsOnProxyProtocolListener() throws Exception {
    final String host = server.getProxyProtocolListeners().get(0).getHost();
    final int port = server.getProxyProtocolListeners().get(0).getPort();

    Socket clientSocket = new Socket(host, port);
    BufferedWriter out = new BufferedWriter(
        new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8));
    InputStreamReader in =
        new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8);
    out.write("GET /info HTTP/1.0\r\n");
    out.write("Host: " + host + "\r\n");
    out.write("\r\n");
    out.flush();
    // As specified in the PROXY protocol specification:
    // The protocol is not covered by the PROXY protocol specification and the connection must be dropped.
    assert (in.read() == -1);
    out.close();
    in.close();
    clientSocket.close();
  }

  @Test
  public void shouldNotHandleProxyProtocolRequestsOnRegularListener() throws Exception {
    final String host = server.getListeners().get(0).getHost();
    final int port = server.getListeners().get(0).getPort();

    Socket clientSocket = new Socket(host, port);
    BufferedWriter out = new BufferedWriter(
        new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8)
    );
    BufferedReader in = new BufferedReader(
        new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8)
    );
    out.write("PROXY TCP4 198.51.100.22 127.0.0.1 4334 " + port + "\r\n");
    out.write("GET /info HTTP/1.0\r\n");
    out.write("Host: " + host + "\r\n");
    out.write("\r\n");
    out.flush();
    String s = in.readLine();
    assertThat(s, startsWith("HTTP/1.0 400 Bad Request"));
    out.close();
    in.close();
    clientSocket.close();
  }


}
