/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.client.properties.LocalProperties;
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
import java.util.function.Function;
import javax.naming.AuthenticationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.compress.utils.IOUtils;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class KsqlRestClient implements Closeable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final KsqlErrorMessage UNAUTHORIZED_ERROR_MESSAGE = new KsqlErrorMessage(
      Errors.ERROR_CODE_UNAUTHORIZED,
      new AuthenticationException(
          "Could not authenticate successfully with the supplied credentials.")
  );

  private static final KsqlErrorMessage FORBIDDEN_ERROR_MESSAGE = new KsqlErrorMessage(
      Errors.ERROR_CODE_FORBIDDEN,
      new AuthenticationException("You are forbidden from using this cluster.")
  );

  private final Client client;

  private URI serverAddress;

  private final LocalProperties localProperties;

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
    this.localProperties = new LocalProperties(localProperties);
  }

  public void setupAuthenticationCredentials(final String userName, final String password) {
    final HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(
        Objects.requireNonNull(userName),
        Objects.requireNonNull(password)
    );
    client.register(feature);
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
    return getRequest("/info", ServerInfo.class);
  }

  public RestResponse<KsqlEntityList> makeKsqlRequest(final String ksql) {
    final KsqlRequest jsonRequest = new KsqlRequest(ksql, localProperties.toMap());
    return postRequest("ksql", jsonRequest, true, r -> r.readEntity(KsqlEntityList.class));
  }

  public RestResponse<CommandStatuses> makeStatusRequest() {
    return getRequest("status", CommandStatuses.class);
  }

  public RestResponse<CommandStatus> makeStatusRequest(final String commandId) {
    return getRequest(String.format("status/%s", commandId), CommandStatus.class);
  }

  public RestResponse<QueryStream> makeQueryRequest(final String ksql) {
    final KsqlRequest jsonRequest = new KsqlRequest(ksql, localProperties.toMap());
    return postRequest("query", jsonRequest, false, QueryStream::new);
  }

  public RestResponse<InputStream> makePrintTopicRequest(final String ksql) {
    final KsqlRequest jsonRequest = new KsqlRequest(ksql, localProperties.toMap());
    return postRequest("query", jsonRequest, false, r -> (InputStream)r.getEntity());
  }

  @Override
  public void close() {
    client.close();
  }

  private <T> RestResponse<T> getRequest(final String path, final Class<T> type) {

    try (Response response = client.target(serverAddress)
        .path(path)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .get()) {

      return response.getStatus() == Response.Status.OK.getStatusCode()
          ? RestResponse.successful(response.readEntity(type))
          : createErrorResponse(path, response);

    } catch (final Exception e) {
      throw new KsqlRestClientException("Error issuing GET to KSQL server. path:" + path, e);
    }
  }

  private <T> RestResponse<T> postRequest(
      final String path,
      final Object jsonEntity,
      final boolean closeResponse,
      final Function<Response, T> mapper) {

    Response response = null;

    try {
      response = client.target(serverAddress)
          .path(path)
          .request(MediaType.APPLICATION_JSON_TYPE)
          .post(Entity.json(jsonEntity));

      return response.getStatus() == Response.Status.OK.getStatusCode()
          ? RestResponse.successful(mapper.apply(response))
          : createErrorResponse(path, response);

    } catch (final Exception e) {
      throw new KsqlRestClientException("Error issuing POST to KSQL server. path:" + path, e);
    } finally {
      if (response != null && closeResponse) {
        response.close();
      }
    }
  }

  private static <T> RestResponse<T> createErrorResponse(
      final String path,
      final Response response) {

    final KsqlErrorMessage errorMessage = response.readEntity(KsqlErrorMessage.class);
    if (errorMessage != null) {
      return RestResponse.erroneous(errorMessage);
    }

    if (response.getStatus() == Status.NOT_FOUND.getStatusCode()) {
      return RestResponse.erroneous(404, "Path not found. Path='" + path + "'. "
          + "Check your ksql http url to make sure you are connecting to a ksql server.");
    }

    if (response.getStatus() == Status.UNAUTHORIZED.getStatusCode()) {
      return RestResponse.erroneous(UNAUTHORIZED_ERROR_MESSAGE);
    }

    if (response.getStatus() == Status.FORBIDDEN.getStatusCode()) {
      return RestResponse.erroneous(FORBIDDEN_ERROR_MESSAGE);
    }

    return RestResponse.erroneous(
        Errors.toErrorCode(response.getStatus()), "The server returned an unexpected error.");
  }

  public static final class QueryStream implements Closeable, Iterator<StreamedRow> {

    private final Response response;
    private final ObjectMapper objectMapper;
    private final Scanner responseScanner;
    private final InputStreamReader isr;

    private StreamedRow bufferedRow;
    private volatile boolean closed = false;

    private QueryStream(final Response response) {
      this.response = response;

      this.objectMapper = new ObjectMapper();
      this.isr = new InputStreamReader(
          (InputStream) response.getEntity(),
          StandardCharsets.UTF_8
      );
      this.responseScanner = new Scanner((buf) -> {
        int wait = 1;
        // poll the input stream's readiness between interruptable sleeps
        // this ensures we cannot block indefinitely on read()
        while (true) {
          if (closed) {
            throw closedIllegalStateException("hasNext()");
          }
          if (isr.ready()) {
            break;
          }
          synchronized (this) {
            if (closed) {
              throw closedIllegalStateException("hasNext()");
            }
            try {
              wait = java.lang.Math.min(wait * 2, 200);
              wait(wait);
            } catch (final InterruptedException e) {
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
        final String responseLine = responseScanner.nextLine().trim();
        if (!responseLine.isEmpty()) {
          try {
            bufferedRow = objectMapper.readValue(responseLine, StreamedRow.class);
          } catch (final IOException exception) {
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

      final StreamedRow result = bufferedRow;
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
      IOUtils.closeQuietly(isr);
    }

    private IllegalStateException closedIllegalStateException(final String methodName) {
      return new IllegalStateException("Cannot call " + methodName + " when QueryStream is closed");
    }
  }

  public Object setProperty(final String property, final Object value) {
    return localProperties.set(property, value);
  }

  public Object unsetProperty(final String property) {
    return localProperties.unset(property);
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
