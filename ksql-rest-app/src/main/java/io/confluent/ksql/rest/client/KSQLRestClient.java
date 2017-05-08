/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.client;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Scanner;


public class KSQLRestClient implements Closeable, AutoCloseable {

  private final Client client;

  private String serverAddress;

  public KSQLRestClient(String serverAddress) {
    this.client = ClientBuilder.newClient();
    this.serverAddress = serverAddress;
  }

  public String getServerAddress() {
    return serverAddress;
  }

  public void setServerAddress(String serverAddress) {
    this.serverAddress = serverAddress;
  }

  public JsonStructure makeKQLRequest(String kql) {
    JsonObject requestData = Json.createObjectBuilder().add("ksql", kql).build();
    return parseJsonResponse(makePostRequest("ksql", requestData));
  }

  public JsonStructure makeStatusRequest() {
    return parseJsonResponse(makeGetRequest("status"));
  }

  public JsonStructure makeStatusRequest(String statementId) {
    return parseJsonResponse(makeGetRequest(String.format("status/%s", statementId)));
  }

  public QueryStream makeQueryRequest(String kql) {
    JsonObject requestData = Json.createObjectBuilder().add("ksql", kql).build();
    return new QueryStream(makePostRequest("query", requestData));
  }

  @Override
  public void close() {
    client.close();
  }

  private Response makePostRequest(String path, JsonValue data) {
    return client.target(serverAddress).path(path)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(data.toString()));
  }

  private Response makeGetRequest(String path) {
    return client.target(serverAddress).path(path)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .get();
  }

  private JsonStructure parseJsonResponse(Response response) {
    return Json.createReader((InputStream) response.getEntity()).read();
  }

  public static class QueryStream implements Closeable, AutoCloseable, Iterator<JsonStructure> {
    private final Response response;
    private final Scanner responseScanner;

    private JsonStructure bufferedRow;
    private boolean closed;

    public QueryStream(Response response) {
      this.response = response;

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
          InputStream responseLineInputStream = new ByteArrayInputStream(responseLine.getBytes());
          bufferedRow = Json.createReader(responseLineInputStream).read();
          return true;
        }
      }

      return false;
    }

    @Override
    public JsonStructure next() {
      if (closed) {
        throw new IllegalStateException("Cannot call next() once closed");
      }

      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      JsonStructure result = bufferedRow;
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
