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

package io.confluent.ksql.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.codehaus.jackson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.rest.entity.PropertiesList;

public class CliUtils {

  private static final Logger log = LoggerFactory.getLogger(CliUtils.class);

  public Optional<String> getAvroSchemaIfAvroTopic(
      SqlBaseParser.RegisterTopicContext registerTopicContext
  ) {
    AstBuilder astBuilder = new AstBuilder(null);
    RegisterTopic registerTopic =
        (RegisterTopic) astBuilder.visitRegisterTopic(registerTopicContext);
    if (registerTopic.getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY) == null) {
      throw new KsqlException("VALUE_FORMAT is not set for the topic.");
    }
    if (registerTopic.getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY).toString()
        .equalsIgnoreCase("'AVRO'")) {
      if (registerTopic.getProperties().containsKey(DdlConfig.AVRO_SCHEMA_FILE)) {
        String avroSchema = getAvroSchema(AstBuilder.unquote(
            registerTopic.getProperties().get(DdlConfig.AVRO_SCHEMA_FILE).toString(), "'")
        );
        return Optional.of(avroSchema);
      } else {
        throw new KsqlException(
            "You need to provide avro schema file path for topics in avro format.");
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
      throw new KsqlException(
          "Could not parse the avro schema file. Details: " + e.getMessage(),
          e
      );
    } catch (IOException e) {
      throw new KsqlException("Could not read the avro schema file. Details: " + e.getMessage(), e);
    }
  }

  public static PropertiesList propertiesListWithOverrides(
      PropertiesList propertiesList,
      Map<String, Object> localProperties
  ) {
    Map<String, Object> properties = propertiesList.getProperties();
    for (Map.Entry<String, Object> localPropertyEntry : localProperties.entrySet()) {
      properties.put(
          "(LOCAL OVERRIDE) " + localPropertyEntry.getKey(),
          localPropertyEntry.getValue()
      );
    }
    return new PropertiesList(propertiesList.getStatementText(), properties);
  }

  public static String getLocalServerAddress(int portNumber) {
    return String.format("http://localhost:%d", portNumber);
  }

  public static boolean createFile(Path path) {
    try {
      final Path parent = path.getParent();
      if (parent == null) {
        log.warn("Failed to create file as the parent was null. path: {}", path);
        return false;
      }
      Files.createDirectories(parent);
      if (Files.notExists(path)) {
        Files.createFile(path);
      }
      return true;
    } catch (Exception e) {
      log.warn("createFile failed, path: {}", path, e);
      return false;
    }
  }
}
