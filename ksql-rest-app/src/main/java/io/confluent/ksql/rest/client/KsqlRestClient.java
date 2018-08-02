/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.Maps;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.resources.Errors;
import io.confluent.ksql.rest.util.JsonMapper;
import io.confluent.rest.validation.JacksonMessageBodyProvider;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Properties;
import java.util.Scanner;
import javax.naming.AuthenticationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

public class KsqlRestClient implements Closeable, AutoCloseable {

  private final Client client;

  private URI serverAddress;

  private final Map<String, Object> localProperties;

  private boolean hasUserCredentials = false;

  public KsqlRestClient(final String serverAddress) {
    this(serverAddress, Collections.emptyMap());
  }

  public KsqlRestClient(final String serverAddress, final Properties properties) {
    this(serverAddress, propertiesToMap(properties));
  }

  public KsqlRestClient(final String serverAddress, final Map<String, Object> localProperties) {
    this(buildClient(), serverAddress, localProperties);
  }

  // Visible for testing
  KsqlRestClient(final Client client,
                 final String serverAddress,
                 final Map<String, Object> localProperties) {
    this.client = Objects.requireNonNull(client, "client");
    this.serverAddress = parseServerAddress(serverAddress);
    this.localProperties = Maps.newHashMap(
        Objects.requireNonNull(localProperties, "localProperties"));
  }

  public void setupAuthenticationCredentials(String userName, String password) {
    HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(
        Objects.requireNonNull(userName),
        Objects.requireNonNull(password)
    );
    client.register(feature);
    hasUserCredentials = true;
  }

  // Visible for testing
  public boolean hasUserCredentials() {
    return hasUserCredentials;
  }

  public URI getServerAddress() {
    return serverAddress;
  }

  public void setServerAddress(final String serverAddress) {
    this.serverAddress = parseServerAddress(serverAddress);
  }

  public RestResponse<ServerInfo> makeRootRequest() {
    return getServerInfo();
  }

  public RestResponse<ServerInfo> getServerInfo() {
    return makeRequest("/info", ServerInfo.class);
  }

  public <T> RestResponse<T> makeRequest(String path, Class<T> type) {
    Response response = makeGetRequest(path);
    try {
      if (response.getStatus() == Response.Status.UNAUTHORIZED.getStatusCode()) {
        return RestResponse.erroneous(
                new KsqlErrorMessage(
                    Errors.ERROR_CODE_UNAUTHORIZED,
                    new AuthenticationException(
                        "Could not authenticate successfully with the supplied credentials."
                    )
                )
        );
      }
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        return RestResponse.erroneous(response.readEntity(KsqlErrorMessage.class));
      }
      T result = response.readEntity(type);
      return RestResponse.successful(result);
    } finally {
      response.close();
    }
  }

  public RestResponse<KsqlEntityList> makeKsqlRequest(String ksql) {
    KsqlRequest jsonRequest = new KsqlRequest(ksql, localProperties);
    Response response = makePostRequest("ksql", jsonRequest);
    try {
      if (response.getStatus() == Response.Status.OK.getStatusCode()) {
        return RestResponse.successful(response.readEntity(KsqlEntityList.class));
      } else {
        return RestResponse.erroneous(response.readEntity(KsqlErrorMessage.class));
      }
    } finally {
      response.close();
    }
  }

  public RestResponse<CommandStatuses> makeStatusRequest() {
    return makeRequest("status", CommandStatuses.class);
  }

  public RestResponse<CommandStatus> makeStatusRequest(String commandId) {
    return makeRequest(String.format("status/%s", commandId), CommandStatus.class);
  }

  public RestResponse<QueryStream> makeQueryRequest(String ksql) {
    KsqlRequest jsonRequest = new KsqlRequest(ksql, localProperties);
    Response response = makePostRequest("query", jsonRequest);
    if (response.getStatus() == Response.Status.OK.getStatusCode()) {
      return RestResponse.successful(new QueryStream(response));
    } else {
      return RestResponse.erroneous(response.readEntity(KsqlErrorMessage.class));
    }
  }

  public RestResponse<InputStream> makePrintTopicRequest(
      String ksql
  ) {
    RestResponse<InputStream> result;
    KsqlRequest jsonRequest = new KsqlRequest(ksql, localProperties);
    Response response = makePostRequest("query", jsonRequest);
    if (response.getStatus() == Response.Status.OK.getStatusCode()) {
      result = RestResponse.successful((InputStream) response.getEntity());
    } else {
      result = RestResponse.erroneous(response.readEntity(KsqlErrorMessage.class));
    }
    return result;
  }

  @Override
  public void close() {
    client.close();
  }

  private Response makePostRequest(String path, Object jsonEntity) {
    try {
      return client.target(serverAddress)
          .path(path)
          .request(MediaType.APPLICATION_JSON_TYPE)
          .post(Entity.json(jsonEntity));
    } catch (Exception exception) {
      throw new KsqlRestClientException("Error issuing POST to KSQL server", exception);
    }
  }

  private Response makeGetRequest(String path) {
    try {
      return client.target(serverAddress).path(path)
          .request(MediaType.APPLICATION_JSON_TYPE)
          .get();
    } catch (Exception exception) {
      throw new KsqlRestClientException("Error issuing GET to KSQL server", exception);
    }
  }

  public static class QueryStream implements Closeable, AutoCloseable, Iterator<StreamedRow> {

    private final Response response;
    private final ObjectMapper objectMapper;
    private final Scanner responseScanner;

    private StreamedRow bufferedRow;
    private volatile boolean closed = false;

    public QueryStream(Response response) {
      this.response = response;

      this.objectMapper = new ObjectMapper();
      InputStreamReader isr = new InputStreamReader(
          (InputStream) response.getEntity(),
          StandardCharsets.UTF_8
      );
      QueryStream stream = this;
      this.responseScanner = new Scanner((buf) -> {
        int wait = 1;
        // poll the input stream's readiness between interruptable sleeps
        // this ensures we cannot block indefinitely on read()
        while (true) {
          if (closed) {
            throw stream.closedIllegalStateException("hasNext()");
          }
          if (isr.ready()) {
            break;
          }
          synchronized (stream) {
            if (closed) {
              throw stream.closedIllegalStateException("hasNext()");
            }
            try {
              wait = java.lang.Math.min(wait * 2, 200);
              stream.wait(wait);
            } catch (InterruptedException e) {
              // this is expected
              // just check the closed flag
            }
          }
        }
        return isr.read(buf);
      });

      this.bufferedRow = null;
    }

    @Override
    public boolean hasNext() {
      if (closed) {
        throw closedIllegalStateException("hasNext()");
      }

      if (bufferedRow != null) {
        return true;
      }

      while (responseScanner.hasNextLine()) {
        String responseLine = responseScanner.nextLine().trim();
        if (!responseLine.isEmpty()) {
          try {
            bufferedRow = objectMapper.readValue(responseLine, StreamedRow.class);
          } catch (IOException exception) {
            // TODO: Should the exception be handled somehow else?
            // Swallowing it silently seems like a bad idea...
            throw new RuntimeException(exception);
          }
          return true;
        }
      }

      return false;
    }

    @Override
    public StreamedRow next() {
      if (closed) {
        throw closedIllegalStateException("next()");
      }

      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      StreamedRow result = bufferedRow;
      bufferedRow = null;
      return result;
    }

    @Override
    public void close() {
      if (closed) {
        throw closedIllegalStateException("close()");
      }

      synchronized (this) {
        closed = true;
        this.notifyAll();
      }
      responseScanner.close();
      response.close();
    }

    private IllegalStateException closedIllegalStateException(String methodName) {
      return new IllegalStateException("Cannot call " + methodName + " when QueryStream is closed");
    }
  }

  public Map<String, Object> getLocalProperties() {
    return localProperties;
  }

  public Object setProperty(String property, Object value) {
    return localProperties.put(property, value);
  }

  public boolean unsetProperty(String property) {
    return localProperties.remove(property) != null;
  }

  private static Map<String, Object> propertiesToMap(final Properties properties) {
    final Map<String, Object> propertiesMap = new HashMap<>();
    properties.stringPropertyNames().forEach(
        prop -> propertiesMap.put(prop, properties.getProperty(prop)));

    return propertiesMap;
  }

  private static URI parseServerAddress(final String serverAddress) {
    Objects.requireNonNull(serverAddress, "serverAddress");
    try {
      return new URL(serverAddress).toURI();
    } catch (final Exception e) {
      throw new KsqlRestClientException(
          "The supplied serverAddress is invalid: " + serverAddress, e);
    }
  }

  private static Client buildClient() {
    final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
    objectMapper.registerModule(new Jdk8Module());
    final JacksonMessageBodyProvider jsonProvider = new JacksonMessageBodyProvider(objectMapper);
    return ClientBuilder.newBuilder().register(jsonProvider).build();
  }
}
