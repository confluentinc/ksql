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

package io.confluent.ksql.rest.entity;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_ABSENT)
@JsonSubTypes({})
public final class StreamedRow {

  private static final ObjectMapper OBJECT_MAPPER = ApiJsonMapper.INSTANCE.get();

  private final Optional<Header> header;
  private final Optional<GenericRow> row;
  private final Optional<KsqlErrorMessage> errorMessage;
  private final Optional<String> finalMessage;
  private final Optional<KsqlHostInfoEntity> sourceHost;

  public static StreamedRow header(final QueryId queryId, final LogicalSchema schema) {
    return new StreamedRow(
        Optional.of(Header.of(queryId, schema)),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
  }

  public static StreamedRow row(final GenericRow row) {
    return new StreamedRow(
        Optional.empty(),
        Optional.of(row),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
  }

  public static StreamedRow row(final GenericRow row,
      final Optional<KsqlHostInfoEntity> sourceHost) {
    return new StreamedRow(
        Optional.empty(),
        Optional.of(row),
        Optional.empty(),
        Optional.empty(),
        sourceHost
    );
  }

  public static StreamedRow error(final Throwable exception, final int errorCode) {
    return new StreamedRow(
        Optional.empty(),
        Optional.empty(),
        Optional.of(new KsqlErrorMessage(errorCode, exception)),
        Optional.empty(),
        Optional.empty()
    );
  }

  public static StreamedRow finalMessage(final String finalMessage) {
    return new StreamedRow(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(finalMessage),
        Optional.empty()
    );
  }

  @JsonCreator
  private StreamedRow(
      @JsonProperty("header") final Optional<Header> header,
      @JsonProperty("row") final Optional<GenericRow> row,
      @JsonProperty("errorMessage") final Optional<KsqlErrorMessage> errorMessage,
      @JsonProperty("finalMessage") final Optional<String> finalMessage,
      @JsonProperty("sourceHost") final Optional<KsqlHostInfoEntity> sourceHost
  ) {
    this.header = requireNonNull(header, "header");
    this.row = requireNonNull(row, "row");
    this.errorMessage = requireNonNull(errorMessage, "errorMessage");
    this.finalMessage = requireNonNull(finalMessage, "finalMessage");
    this.sourceHost = requireNonNull(sourceHost, "sourceHost");

    checkUnion(header, row, errorMessage, finalMessage);
  }

  public Optional<Header> getHeader() {
    return header;
  }

  public Optional<GenericRow> getRow() {
    return row;
  }

  public Optional<KsqlErrorMessage> getErrorMessage() {
    return errorMessage;
  }

  public Optional<String> getFinalMessage() {
    return finalMessage;
  }

  public Optional<KsqlHostInfoEntity> getSourceHost() {
    return sourceHost;
  }

  @JsonIgnore
  public boolean isTerminal() {
    return finalMessage.isPresent() || errorMessage.isPresent();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamedRow that = (StreamedRow) o;
    return Objects.equals(header, that.header)
        && Objects.equals(row, that.row)
        && Objects.equals(errorMessage, that.errorMessage)
        && Objects.equals(finalMessage, that.finalMessage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(header, row, errorMessage, finalMessage);
  }

  @Override
  public String toString() {
    try {
      return OBJECT_MAPPER.writeValueAsString(this);
    } catch (final JsonProcessingException e) {
      return super.toString();
    }
  }

  private static void checkUnion(final Optional<?>... fs) {
    final long count = Arrays.stream(fs)
        .filter(Optional::isPresent)
        .count();

    if (count != 1) {
      throw new IllegalArgumentException("Exactly one parameter should be non-null. got: " + count);
    }
  }

  @Immutable
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class Header {

    private final QueryId queryId;
    private final LogicalSchema schema;

    @JsonCreator
    public static Header of(
        @JsonProperty(value = "queryId", required = true) final QueryId queryId,
        @JsonProperty(value = "schema", required = true) final LogicalSchema schema
    ) {
      return new Header(queryId, schema);
    }

    public QueryId getQueryId() {
      return queryId;
    }

    public LogicalSchema getSchema() {
      return schema;
    }

    private Header(final QueryId queryId, final LogicalSchema schema) {
      this.queryId = requireNonNull(queryId, "queryId");
      this.schema = requireNonNull(schema, "schema");
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Header header = (Header) o;
      return Objects.equals(queryId, header.queryId)
          && Objects.equals(schema, header.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(queryId, schema);
    }

    @Override
    public String toString() {
      try {
        return OBJECT_MAPPER.writeValueAsString(this);
      } catch (final JsonProcessingException e) {
        return super.toString();
      }
    }
  }
}
