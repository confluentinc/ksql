/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.ErrorMessage;
import io.confluent.ksql.rest.entity.KSQLEntity;
import io.confluent.ksql.rest.entity.KSQLEntityList;
import io.confluent.ksql.rest.entity.KSQLRequest;
import io.confluent.ksql.rest.entity.SchemaMapper;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.rest.validation.JacksonMessageBodyProvider;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;

public class KSQLRestClient implements Closeable, AutoCloseable {

  private final Client client;

  private String serverAddress;

  public KSQLRestClient(String serverAddress) {
    this.serverAddress = serverAddress;

    ObjectMapper objectMapper = new SchemaMapper().registerToObjectMapper(new ObjectMapper());
    JacksonMessageBodyProvider jsonProvider = new JacksonMessageBodyProvider(objectMapper);
    this.client = ClientBuilder.newBuilder().register(jsonProvider).build();
  }

  public String getServerAddress() {
    return serverAddress;
  }

  public void setServerAddress(String serverAddress) {
    this.serverAddress = serverAddress;
  }

  public RestResponse<List<KSQLEntity>> makeKSQLRequest(String ksql) {
    KSQLRequest jsonRequest = new KSQLRequest(ksql);
    Response response = makePostRequest("ksql", jsonRequest);
    List<KSQLEntity> result = response.readEntity(KSQLEntityList.class);
    response.close();
    return RestResponse.successful(result);
  }

  public RestResponse<Map<CommandId, CommandStatus.Status>> makeStatusRequest() {
    Response response = makeGetRequest("status");
    Map<CommandId, CommandStatus.Status> result = response.readEntity(CommandStatuses.class);
    response.close();
    return RestResponse.successful(result);
  }

  public RestResponse<CommandStatus> makeStatusRequest(String commandId) {
    RestResponse<CommandStatus> result;
    Response response = makeGetRequest(String.format("status/%s", commandId));
    if (response.getStatus() == Response.Status.OK.getStatusCode()) {
      result = RestResponse.successful(response.readEntity(CommandStatus.class));
    } else {
      result = RestResponse.erroneous(response.readEntity(ErrorMessage.class));
    }
    response.close();
    return result;
  }

  public RestResponse<QueryStream> makeQueryRequest(String ksql) {
    KSQLRequest jsonRequest = new KSQLRequest(ksql);
    return RestResponse.successful(new QueryStream(makePostRequest("query", jsonRequest)));
  }

  public RestResponse<Response> makePrintTopicRequest(String ksql) {
    RestResponse<Response> result;
    KSQLRequest jsonRequest = new KSQLRequest(ksql);
    Response response = makePostRequest("query", jsonRequest);
    if (response.getStatus() == Response.Status.OK.getStatusCode()) {
      result = RestResponse.successful(response);
    } else {
      result = RestResponse.erroneous(response.readEntity(ErrorMessage.class));
    }
    return result;
  }

  @Override
  public void close() {
    client.close();
  }

  private Response makePostRequest(String path, Object jsonEntity) {
    return client.target(serverAddress)
        .path(path)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.json(jsonEntity));
  }

  private Response makeGetRequest(String path) {
    return client.target(serverAddress).path(path)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .get();
  }

  public static class QueryStream implements Closeable, AutoCloseable, Iterator<GenericRow> {
    private final Response response;
    private final ObjectMapper objectMapper;
    private final Scanner responseScanner;

    private GenericRow bufferedRow;
    private boolean closed;

    public QueryStream(Response response) {
      this.response = response;

      this.objectMapper = new ObjectMapper();
      this.responseScanner = new Scanner((InputStream) response.getEntity());

      this.bufferedRow = null;
      this.closed = false;
    }

    @Override
    public boolean hasNext() {
      if (closed) {
        throw new IllegalStateException("Cannot call hasNext() once closed");
      }

      if (bufferedRow != null) {
        return true;
      }

      while (responseScanner.hasNextLine()) {
        String responseLine = responseScanner.nextLine().trim();
        if (!responseLine.isEmpty()) {
          try {
            bufferedRow = objectMapper.readValue(responseLine, GenericRow.class);
          } catch (IOException exception) {
            return false;
          }
          return true;
        }
      }

      return false;
    }

    @Override
    public GenericRow next() {
      if (closed) {
        throw new IllegalStateException("Cannot call next() once closed");
      }

      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      GenericRow result = bufferedRow;
      bufferedRow = null;
      return result;
    }

    @Override
    public void close() {
      if (closed) {
        throw new IllegalStateException("Cannot call close() when already closed");
      }

      closed = true;
      responseScanner.close();
      response.close();
    }
  }
}
