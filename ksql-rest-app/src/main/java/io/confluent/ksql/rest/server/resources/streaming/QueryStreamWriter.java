/*
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

import com.google.common.collect.Lists;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.StreamingOutput;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;

class QueryStreamWriter implements StreamingOutput {

  private static final Logger log = LoggerFactory.getLogger(QueryStreamWriter.class);

  private final QueuedQueryMetadata queryMetadata;
  private final long disconnectCheckInterval;
  private final ObjectMapper objectMapper;
  private final KsqlEngine ksqlEngine;
  private volatile Exception streamsException;
  private volatile boolean limitReached = false;

  QueryStreamWriter(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final long disconnectCheckInterval,
      final String queryString,
      final Map<String, Object> overriddenProperties,
      final ObjectMapper objectMapper
  ) throws Exception {
    QueryMetadata queryMetadata =
        ksqlEngine.buildMultipleQueries(
            queryString, ksqlConfig, overriddenProperties).get(0);
    this.objectMapper = objectMapper;
    if (!(queryMetadata instanceof QueuedQueryMetadata)) {
      throw new Exception(String.format(
          "Unexpected metadata type: expected QueuedQueryMetadata, found %s instead",
          queryMetadata.getClass()
      ));
    }

    this.disconnectCheckInterval = disconnectCheckInterval;
    this.queryMetadata = ((QueuedQueryMetadata) queryMetadata);
    this.queryMetadata.setLimitHandler(new LimitHandler());
    this.queryMetadata.getKafkaStreams().setUncaughtExceptionHandler(new StreamsExceptionHandler());
    this.ksqlEngine = ksqlEngine;

    queryMetadata.getKafkaStreams().start();
  }

  @Override
  public void write(OutputStream out) {
    try {
      while (queryMetadata.isRunning() && !limitReached) {
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
        drainAndThrowOnError(out);
      }

      drain(out);

      if (limitReached) {
        objectMapper.writeValue(out, StreamedRow.finalMessage("Limit Reached"));
        out.write("\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
      }
    } catch (final EOFException exception) {
      // The user has terminated the connection; we can stop writing
      log.warn("Query terminated due to exception:" + exception.toString());
    } catch (final InterruptedException exception) {
      // The most likely cause of this is the server shutting down. Should just try to close
      // gracefully, without writing any more to the connection stream.
      log.warn("Interrupted while writing to connection stream");
    } catch (final Exception exception) {
      log.error("Exception occurred while writing to connection stream: ", exception);
      outputException(out, exception);
    } finally {
      ksqlEngine.removeTemporaryQuery(queryMetadata);
      queryMetadata.close();
      queryMetadata.cleanUpInternalTopicAvroSchemas(ksqlEngine.getSchemaRegistryClient());
    }
  }

  private void write(OutputStream output, GenericRow row) throws IOException {
    objectMapper.writeValue(output, StreamedRow.row(row));
    output.write("\n".getBytes(StandardCharsets.UTF_8));
    output.flush();
  }

  private void outputException(final OutputStream out, final Throwable exception) {
    try {
      out.write("\n".getBytes(StandardCharsets.UTF_8));
      if (exception.getCause() instanceof KsqlException) {
        objectMapper.writeValue(out, StreamedRow.error(exception.getCause()));
      } else {
        objectMapper.writeValue(out, StreamedRow.error(exception));
      }
      out.write("\n".getBytes(StandardCharsets.UTF_8));
      out.flush();
    } catch (final IOException e) {
      log.debug("Client disconnected while attempting to write an error message");
    }
  }

  private void drainAndThrowOnError(final OutputStream out) throws Exception {
    if (streamsException != null) {
      drain(out);
      throw streamsException;
    }
  }

  private void drain(final OutputStream out) throws IOException {
    final List<KeyValue<String, GenericRow>> rows = Lists.newArrayList();
    queryMetadata.getRowQueue().drainTo(rows);

    for (final KeyValue<String, GenericRow> row : rows) {
      write(out, row.value);
    }
  }

  private class StreamsExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread thread, Throwable exception) {
      streamsException = exception instanceof Exception
          ? (Exception) exception
          : new RuntimeException(exception);
    }
  }

  private class LimitHandler implements OutputNode.LimitHandler {
    @Override
    public void limitReached() {
      limitReached = true;
    }
  }
}
