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

package io.confluent.ksql.rest.server.resources.streaming;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.StreamingOutput;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;

class QueryStreamWriter implements StreamingOutput {

  private static final Logger log = LoggerFactory.getLogger(QueryStreamWriter.class);

  private final QueuedQueryMetadata queryMetadata;
  private final long disconnectCheckInterval;
  private final ObjectMapper objectMapper;
  private Throwable streamsException;
  private final KsqlEngine ksqlEngine;


  QueryStreamWriter(
      KsqlEngine ksqlEngine,
      long disconnectCheckInterval,
      String queryString,
      Map<String, Object> overriddenProperties
  ) throws Exception {
    QueryMetadata queryMetadata =
        ksqlEngine.buildMultipleQueries(queryString, overriddenProperties).get(0);
    this.objectMapper = new ObjectMapper().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
    if (!(queryMetadata instanceof QueuedQueryMetadata)) {
      throw new Exception(String.format(
          "Unexpected metadata type: expected QueuedQueryMetadata, found %s instead",
          queryMetadata.getClass()
      ));
    }

    this.disconnectCheckInterval = disconnectCheckInterval;
    this.queryMetadata = ((QueuedQueryMetadata) queryMetadata);

    this.queryMetadata.getKafkaStreams().setUncaughtExceptionHandler(new StreamsExceptionHandler());
    this.ksqlEngine = ksqlEngine;
    queryMetadata.getKafkaStreams().start();
  }

  @Override
  public void write(OutputStream out) throws IOException {
    try {
      while (true) {
        KeyValue<String, GenericRow> value = queryMetadata.getRowQueue().poll(
            disconnectCheckInterval,
            TimeUnit.MILLISECONDS
        );
        if (value != null) {
          write(out, value.value);
        } else {
          // If no new rows have been written, the user may have terminated the connection without
          // us knowing. Check by trying to write a single newline.
          out.write("\n".getBytes(StandardCharsets.UTF_8));
          out.flush();
        }
        if (streamsException != null) {
          throw streamsException;
        }
      }
    } catch (EOFException exception) {
      // The user has terminated the connection; we can stop writing
      log.warn("Query terminated due to exception:" + exception.toString());
    } catch (InterruptedException exception) {
      // The most likely cause of this is the server shutting down. Should just try to close
      // gracefully, without writing any more to the connection stream.
      log.warn("Interrupted while writing to connection stream");
    } catch (Throwable exception) {
      log.error("Exception occurred while writing to connection stream: ", exception);
      out.write("\n".getBytes(StandardCharsets.UTF_8));
      if (exception.getCause() instanceof KsqlException) {
        objectMapper.writeValue(out, new StreamedRow(exception.getCause()));
      } else {
        objectMapper.writeValue(out, new StreamedRow(exception));
      }
      out.write("\n".getBytes(StandardCharsets.UTF_8));
      out.flush();

    } finally {
      ksqlEngine.removeTemporaryQuery(queryMetadata);
      queryMetadata.close();
      queryMetadata.cleanUpInternalTopicAvroSchemas(ksqlEngine.getSchemaRegistryClient());
    }
  }

  private void write(OutputStream output, GenericRow row) throws IOException {
    objectMapper.writeValue(output, new StreamedRow(row));
    output.write("\n".getBytes(StandardCharsets.UTF_8));
    output.flush();
  }

  private class StreamsExceptionHandler implements Thread.UncaughtExceptionHandler {

    @Override
    public void uncaughtException(Thread thread, Throwable exception) {
      streamsException = exception;
    }
  }
}
