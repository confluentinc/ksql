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

package io.confluent.ksql.api.server;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.rest.entity.ConsistencyToken;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.PushContinuationToken;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.vertx.core.http.HttpServerResponse;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the query response stream in delimited format.
 *
 * <p>The response comprises a sequence of elements, separated by newline. The overall response
 * does
 * not form a single JSON object or array. This makes it easier to parse at the client without
 * recourse to a streaming JSON parser.
 *
 * <p>The first entry in the response is a JSON object representing the metadata of the query.
 * It contains the column names, column types, query ID, and number of rows (in the case of a pull
 * query).
 *
 * <p>Each subsequent entry in the stream is a JSON array representing the values of the columns
 * returned by the query.
 *
 * <p>Please consult the API documentation for a full description of the format.
 */
public class DelimitedQueryStreamResponseWriter implements QueryStreamResponseWriter {
  private static final Logger LOG
      = LoggerFactory.getLogger(DelimitedQueryStreamResponseWriter.class);

  private final HttpServerResponse response;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public DelimitedQueryStreamResponseWriter(final HttpServerResponse response) {
    this.response = Objects.requireNonNull(response);
  }

  @Override
  public QueryStreamResponseWriter writeMetadata(final QueryResponseMetadata metaData) {
    response.write(ServerUtils.serializeObject(metaData).appendString("\n"));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeRow(
      final KeyValueMetadata<List<?>, GenericRow> keyValueMetadata
  ) {
    final KeyValue<List<?>, GenericRow> keyValue = keyValueMetadata.getKeyValue();
    if (keyValue.value() == null) {
      LOG.warn("Dropped tombstone. Not currently supported");
    } else {
      response.write(ServerUtils.serializeObject(keyValue.value().values()).appendString("\n"));
    }
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeContinuationToken(
      final PushContinuationToken pushContinuationToken) {
    response.write(ServerUtils.serializeObject(pushContinuationToken).appendString("\n"));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeError(final KsqlErrorMessage error) {
    response.write(ServerUtils.serializeObject(error).appendString("\n"));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeConsistencyToken(final ConsistencyToken consistencyToken) {
    response.write(ServerUtils.serializeObject(consistencyToken).appendString("\n"));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeCompletionMessage() {
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeLimitMessage() {
    return this;
  }


  @Override
  public void end() {
    response.end();
  }
}
