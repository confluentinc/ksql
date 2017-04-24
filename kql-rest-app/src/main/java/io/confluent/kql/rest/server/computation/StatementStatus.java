package io.confluent.kql.rest.server.computation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Map;
import java.util.Objects;

public class StatementStatus {
  public enum Status { QUEUED, PARSING, EXECUTING, RUNNING, TERMINATED, SUCCESS, ERROR; }

  private final Status status;
  private final String message;

  public StatementStatus(Status status, String message) {
    this.status = status;
    this.message = message;
  }

  public Status getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }

  public static class StatementStatusSerializer implements Serializer<StatementStatus> {
    public static final String STATUS_FIELD = "status";
    public static final String MESSAGE_FIELD = "message";

    private final JsonSerializer jsonSerializer;

    public StatementStatusSerializer() {
      jsonSerializer = new JsonSerializer();
    }

    @Override
    public void close() {
      jsonSerializer.close();
    }

    @Override
    public void configure(Map<String, ?> properties, boolean isKey) {
      jsonSerializer.configure(properties, isKey);
    }

    @Override
    public byte[] serialize(String topic, StatementStatus statementStatus) {
      if (statementStatus == null) {
        return null;
      }

      ObjectNode result = JsonNodeFactory.instance.objectNode();

      result.put(STATUS_FIELD, statementStatus.getStatus().name());
      result.put(MESSAGE_FIELD, statementStatus.getMessage());

      return jsonSerializer.serialize(topic, result);
    }
  }

  public static class StatementStatusDeserializer implements Deserializer<StatementStatus> {
    private final JsonDeserializer jsonDeserializer;

    public StatementStatusDeserializer() {
      this.jsonDeserializer = new JsonDeserializer();
    }

    @Override
    public void close() {
      jsonDeserializer.close();
    }

    @Override
    public void configure(Map<String, ?> properties, boolean isKey) {
      jsonDeserializer.configure(properties, isKey);
    }

    @Override
    public StatementStatus deserialize(String topic, byte[] data) {
      if (data == null) {
        return null;
      }

      JsonNode jsonData = jsonDeserializer.deserialize(topic, data);
      if (!jsonData.isObject()) {
        throw new RuntimeException(String.format(
            "Expected JSON object, found %s instead",
            jsonData
        ));
      }

      ObjectNode jsonObject = (ObjectNode) jsonData;

      String statusString = getStringField(StatementStatusSerializer.STATUS_FIELD, jsonObject);
      Status status;
      try {
        status = Status.valueOf(statusString);
      } catch (IllegalArgumentException exception) {
        throw new RuntimeException(String.format(
            "Unexpected value for %s field: '%s'",
            StatementStatusSerializer.STATUS_FIELD,
            statusString
        ));
      }

      String message = getStringField(StatementStatusSerializer.MESSAGE_FIELD, jsonObject);

      return new StatementStatus(status, message);
    }

    private String getStringField(String field, ObjectNode object) {
      JsonNode result = object.get(field);
      Objects.requireNonNull(result, String.format("Field %s required", field));
      if (!result.isTextual()) {
        throw new RuntimeException(String.format(
            "Expected field %s to be a string, found %s instead",
            field,
            object
        ));
      }
      return result.asText();
    }
  }

  public static class StatementStatusSerde implements Serde<StatementStatus> {
    private final StatementStatusDeserializer deserializer;
    private final StatementStatusSerializer serializer;

    public StatementStatusSerde() {
      this.deserializer = new StatementStatusDeserializer();
      this.serializer = new StatementStatusSerializer();
    }

    @Override
    public void close() {
      deserializer.close();
      serializer.close();
    }

    @Override
    public void configure(Map<String, ?> properties, boolean isKey) {
      deserializer.configure(properties, isKey);
      serializer.configure(properties, isKey);
    }

    @Override
    public Deserializer<StatementStatus> deserializer() {
      return deserializer;
    }

    @Override
    public Serializer<StatementStatus> serializer() {
      return serializer;
    }
  }
}
