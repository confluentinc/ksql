/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.cli.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.codehaus.jackson.JsonParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.util.KsqlException;

public class CliUtils {

  public Optional<String> getAvroSchemaIfAvroTopic(SqlBaseParser.RegisterTopicContext
                                                      registerTopicContext) {
    AstBuilder astBuilder = new AstBuilder(null);
    RegisterTopic registerTopic = (RegisterTopic) astBuilder.visitRegisterTopic(registerTopicContext);
    if (registerTopic.getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY).toString()
        .equalsIgnoreCase("'AVRO'")) {
      if (registerTopic.getProperties().containsKey(DdlConfig.AVRO_SCHEMA_FILE)) {
        String avroSchema = getAvroSchema(AstBuilder.unquote(registerTopic.getProperties()
                                                                 .get(DdlConfig.AVRO_SCHEMA_FILE)
                                                                 .toString(), "'"));
        return Optional.of(avroSchema);
      } else {
        throw new KsqlException("You need to provide avro schema file path for topics in avro format.");
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
    } catch (JsonParseException e) {
      throw new KsqlException("Could not parse the avro schema file.");
    } catch (IOException e) {
      throw new KsqlException("Could not read the avro schema file.");
    }
  }

  public String readQueryFile(final String queryFilePath) throws IOException {
    StringBuilder sb = new StringBuilder();
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(queryFilePath));
      String line = br.readLine();
      while (line != null) {
        sb.append(line);
        sb.append(System.lineSeparator());
        line = br.readLine();
      }
    } catch (IOException e) {
      throw new KsqlException("Could not read the query file.");
    } finally {
      if (br != null) {
        br.close();
      }
    }
    return sb.toString();
  }

  public static String getErrorMessage(Throwable e) {
    if (e instanceof ConnectException) {
      return "Could not connect to the server.";
    } else {
      return e.getMessage();
    }
  }

}
