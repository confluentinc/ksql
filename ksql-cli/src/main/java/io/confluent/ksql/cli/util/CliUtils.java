/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.cli.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.CreateTopic;
import io.confluent.ksql.util.KsqlException;

public class CliUtils {

  public Optional<String> getAvroSchemaIfAvroTopic(SqlBaseParser.CreateTopicContext
                                                      createTopicContext) {
    AstBuilder astBuilder = new AstBuilder(null);
    CreateTopic createTopic = (CreateTopic) astBuilder.visitCreateTopic(createTopicContext);
    if (createTopic.getProperties().get("FORMAT").toString()
        .equalsIgnoreCase("'AVRO'")) {
      if (createTopic.getProperties().containsKey("AVROSCHEMAFILE")) {
        String avroSchema = getAvroSchema(AstBuilder.unquote(createTopic.getProperties()
                                                                 .get("AVROSCHEMAFILE")
                                                                 .toString(), "'"));
        return Optional.of(avroSchema);
      } else {
        throw new KsqlException("You need to provide avro schema file path for topics in avro "
                                + "format.");
      }
    }
    return Optional.empty();
  }

  public String getAvroSchema(final String schemaFilePath) {
    try {
      byte[] jsonData = Files.readAllBytes(Paths.get(schemaFilePath));
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode root = objectMapper.readTree(jsonData);
      return root.toString();
    } catch (IOException e) {
      throw new KsqlException("Could not read the avro schema file.");
    }
  }

}
