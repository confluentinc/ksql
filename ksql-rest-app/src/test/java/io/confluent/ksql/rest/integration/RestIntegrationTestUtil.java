/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.integration;

import static org.junit.Assert.assertEquals;

import com.google.common.net.UrlEscapers;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.test.util.secure.Credentials;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.glassfish.jersey.internal.util.Base64;

final class RestIntegrationTestUtil {

  private RestIntegrationTestUtil() {
  }

  static List<KsqlEntity> makeKsqlRequest(final TestKsqlRestApp restApp, final String sql) {
    return makeKsqlRequest(restApp, sql, Optional.empty());
  }

  static List<KsqlEntity> makeKsqlRequest(
      final TestKsqlRestApp restApp,
      final String sql,
      Optional<Credentials> userCreds
  ) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient()) {
      userCreds.ifPresent(
        creds -> restClient.setupAuthenticationCredentials(creds.username, creds.password)
      );

      final RestResponse<KsqlEntityList> res = restClient.makeKsqlRequest(sql);

      throwOnError(res);

      return awaitResults(restApp, res.getResponse());
    }
  }

  static void createStreams(final TestKsqlRestApp restApp, final String streamName, final String topicName) {
    final Client client = TestKsqlRestApp.buildClient();

    try (final Response response = client
        .target(restApp.getHttpListener())
        .path("ksql")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(ksqlRequest(
            "CREATE STREAM " + streamName + " "
                + "(viewtime bigint, pageid varchar, userid varchar) "
                + "WITH (kafka_topic='" + topicName + "', value_format='json');"))) {

      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    } finally {
      client.close();
    }
  }

  static Entity<?> ksqlRequest(final String sql) {
    return Entity.json(new KsqlRequest(sql, Collections.emptyMap(), null));
  }

  private static List<KsqlEntity> awaitResults(final TestKsqlRestApp restApp, final List<KsqlEntity> pending) {
    try (final KsqlRestClient ksqlRestClient = restApp.buildKsqlClient()) {
      return pending.stream()
          .map(e -> awaitResult(e, ksqlRestClient))
          .collect(Collectors.toList());
    }
  }

  private static KsqlEntity awaitResult(
      final KsqlEntity e,
      final KsqlRestClient ksqlRestClient
  ) {
    if (!(e instanceof CommandStatusEntity)) {
      return e;
    }

    CommandStatusEntity cse = (CommandStatusEntity) e;
    final String commandId = cse.getCommandId().toString();

    while (cse.getCommandStatus().getStatus() != Status.ERROR
        && cse.getCommandStatus().getStatus() != Status.SUCCESS) {

      final RestResponse<CommandStatus> res = ksqlRestClient.makeStatusRequest(commandId);

      throwOnError(res);

      cse = new CommandStatusEntity(
          cse.getStatementText(),
          cse.getCommandId(),
          res.getResponse(),
          cse.getCommandSequenceNumber()
      );
    }

    return cse;
  }

  private static void throwOnError(final RestResponse<?> res) {
    if (res.isErroneous()) {
      throw new AssertionError("Failed to await result."
          + "msg: " + res.getErrorMessage());
    }
  }

  public static WebSocketClient makeWsRequest(
      final URI baseUri,
      final String sql,
      final Object listener,
      final Optional<MediaType> mediaType,
      final Optional<MediaType> contentType,
      final Optional<Credentials> credentials
  ) throws Exception {

    final WebSocketClient wsClient = new WebSocketClient();
    wsClient.start();

    final ClientUpgradeRequest request = new ClientUpgradeRequest();

    credentials.ifPresent(creds -> request
        .setHeader(HttpHeaders.AUTHORIZATION, "Basic " + buildBasicAuthHeader(creds)));

    mediaType.ifPresent(mt -> request.setHeader(HttpHeaders.ACCEPT, mt.toString()));
    contentType.ifPresent(ct -> request.setHeader(HttpHeaders.CONTENT_TYPE, ct.toString()));

    final URI wsUri = baseUri.resolve("/ws/query?request=" + buildStreamingRequest(sql));

    wsClient.connect(listener, wsUri, request);

    return wsClient;
  }

  private static String buildBasicAuthHeader(final Credentials credentials) {
    return Base64.encodeAsString(credentials.username + ":" + credentials.password);
  }

  private static String buildStreamingRequest(final String sql) {
    return UrlEscapers.urlFormParameterEscaper()
        .escape("{"
            + " \"ksql\": \"" + sql + "\""
            + "}");
  }
}
