/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.server.protocol;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.jackson.DatabindCodec;
import java.io.IOException;
import java.util.Optional;

/**
 * Knows how to deserialize and serialize POJOs to buffers. Encapsulates the Jackson encoding logic
 * and exception handling.
 */
public final class PojoCodec {

  private PojoCodec() {
  }

  public static <T> Optional<T> deserialiseObject(final Buffer buffer,
      final PojoDeserializerErrorHandler errorHandler,
      final Class<T> clazz) {
    final ObjectMapper objectMapper = DatabindCodec.mapper();
    try {
      return Optional.of(objectMapper.readValue(buffer.getBytes(), clazz));
    } catch (UnrecognizedPropertyException e) {
      errorHandler.onExtraParam(e.getPropertyName());
      return Optional.empty();
    } catch (MismatchedInputException e) {
      // This is super ugly but I can't see how else to extract the property name
      final int startIndex = e.getMessage().indexOf('\'');
      final int endIndex = e.getMessage().indexOf('\'', startIndex + 1);
      final String propertyName = e.getMessage().substring(startIndex + 1, endIndex);
      errorHandler.onMissingParam(propertyName);
      return Optional.empty();
    } catch (JsonParseException e) {
      errorHandler.onInvalidJson();
      return Optional.empty();
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize buffer", e);
    }
  }

  public static <T> Buffer serializeObject(final T t) {
    final ObjectMapper objectMapper = DatabindCodec.mapper();
    try {
      final byte[] bytes = objectMapper.writeValueAsBytes(t);
      return Buffer.buffer(bytes);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize buffer", e);
    }
  }
}
