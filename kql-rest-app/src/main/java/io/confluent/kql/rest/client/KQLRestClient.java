package io.confluent.kql.rest.client;

import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.json.Json;
import javax.json.JsonObject;
import javax.ws.rs.core.Response;
import java.io.InputStream;


public class KQLRestClient {

  private final Client client;

  private String serverAddress;

  public KQLRestClient(String serverAddress) {
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
    JsonObject requestData = Json.createObjectBuilder().add("kql", kql).build();
    return parseJsonResponse(makePostRequest("kql", requestData));
  }

  public JsonStructure makeStatusRequest() {
    return parseJsonResponse(makeGetRequest("status"));
  }

  public JsonStructure makeStatusRequest(String statementId) {
    return parseJsonResponse(makeGetRequest(String.format("status/%s", statementId)));
  }

  public InputStream makeQueryRequest(String kql) {
    JsonObject requestData = Json.createObjectBuilder().add("kql", kql).build();
    return (InputStream) makePostRequest("query", requestData).getEntity();
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

}
