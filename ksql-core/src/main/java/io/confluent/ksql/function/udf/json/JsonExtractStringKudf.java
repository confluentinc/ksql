/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.function.udf.json;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.json.JsonPathTokenizer;

public class JsonExtractStringKudf implements Kudf {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  String path = null;
  JsonPathTokenizer jsonPathTokenizer = null;
  ImmutableList<String> tokens = null;

  @Override
  public void init() {
  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 2) {
      throw new KsqlFunctionException("getStringFromJson udf should have two input argument.");
    }
    String jsonString = args[0].toString();
    if (path == null) {
      path = args[1].toString();
      jsonPathTokenizer = new JsonPathTokenizer(path);
      tokens = ImmutableList.copyOf(jsonPathTokenizer);
    }
    JsonNode jsonNode = null;
    try {
      jsonNode = OBJECT_MAPPER.readTree(jsonString);
    } catch (IOException e) {
      throw new KsqlException("Invalid JSON format.", e);
    }
    JsonNode currentNode = jsonNode;
    for (String token: tokens) {
      currentNode = currentNode.get(token);
    }
    if (currentNode.isTextual()) {
      return currentNode.asText();
    } else {
      return currentNode.toString();
    }
  }
}
