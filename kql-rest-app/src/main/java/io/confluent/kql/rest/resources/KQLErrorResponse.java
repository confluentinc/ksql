package io.confluent.kql.rest.resources;

import javax.json.Json;
import javax.json.JsonValue;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;

public class KQLErrorResponse {

  /* TODO: Considering adding more programmatic structure to an exception when converting to JSON--maybe something like:
    {
      "message": exception.getMessage(),
      "stack_trace": [element.toString() for element in exception.getStackTrace()],
      if exception.getCause() != null: "cause": exceptionToJson(exception.getCause())
    }
   */
  public static Response stackTraceResponse(Throwable exception) {
    JsonValue entity = stackTraceJson(exception);
    return Response.serverError().entity(entity.toString()).build();
  }

  public static JsonValue stackTraceJson(Throwable exception) {
    StringWriter stringWriter = new StringWriter();
    exception.printStackTrace(new PrintWriter(stringWriter));
    JsonValue stackTrace = Json.createObjectBuilder().add("stack_trace", stringWriter.toString()).build();
    return Json.createObjectBuilder().add("error", stackTrace).build();
  }

  // To be used at a later date when expected exceptions can be separated from unexpected ones
  public static Response errorMessageResponse(Throwable exception) {
    JsonValue entity = errorMessageJson(exception);
    return Response.status(Response.Status.BAD_REQUEST).entity(entity.toString()).build();
  }

  public static JsonValue errorMessageJson(Throwable exception) {
    JsonValue message = Json.createObjectBuilder().add("message", exception.getMessage()).build();
    return Json.createObjectBuilder().add("error", message).build();
  }
}
