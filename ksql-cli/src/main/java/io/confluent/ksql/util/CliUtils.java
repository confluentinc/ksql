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

package io.confluent.ksql.util;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.rest.entity.PropertiesList;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CliUtils {

  private static final Logger log = LoggerFactory.getLogger(CliUtils.class);

  public Optional<String> getAvroSchemaIfAvroTopic(
      final SqlBaseParser.RegisterTopicContext registerTopicContext
  ) {
    final AstBuilder astBuilder = new AstBuilder(null);
    final RegisterTopic registerTopic =
        (RegisterTopic) astBuilder.visitRegisterTopic(registerTopicContext);
    if (registerTopic.getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY) == null) {
      throw new KsqlException("VALUE_FORMAT is not set for the topic.");
    }
    if (registerTopic.getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY).toString()
        .equalsIgnoreCase("'AVRO'")) {
      if (registerTopic.getProperties().containsKey(DdlConfig.AVRO_SCHEMA_FILE)) {
        final String avroSchema = getAvroSchema(AstBuilder.unquote(
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
      final byte[] jsonData = Files.readAllBytes(Paths.get(schemaFilePath));
      final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
      final JsonNode root = objectMapper.readTree(jsonData);
      return root.toString();
    } catch (final JsonParseException e) {
      throw new KsqlException(
          "Could not parse the avro schema file. Details: " + e.getMessage(),
          e
      );
    } catch (final IOException e) {
      throw new KsqlException("Could not read the avro schema file. Details: " + e.getMessage(), e);
    }
  }

  public static List<PropertyDef> propertiesListWithOverrides(final PropertiesList properties) {

    final Function<Entry<String, ?>, PropertyDef> toPropertyDef = e -> {
      final String value = e.getValue() == null ? "NULL" : e.getValue().toString();
      if (properties.getOverwrittenProperties().contains(e.getKey())) {
        return new PropertyDef(e.getKey(), "SESSION", value);
      }

      if (properties.getDefaultProperties().contains(e.getKey())) {
        return new PropertyDef(e.getKey(), "", value);
      }

      return new PropertyDef(e.getKey(), "SERVER", value);
    };

    return properties.getProperties().entrySet().stream()
        .map(toPropertyDef)
        .collect(Collectors.toList());
  }

  public static boolean createFile(final Path path) {
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
    } catch (final Exception e) {
      log.warn("createFile failed, path: {}", path, e);
      return false;
    }
  }
}
