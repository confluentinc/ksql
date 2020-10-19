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
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_ABSENT)
@JsonSubTypes({})
public final class StreamedRow {

  private static final ObjectMapper OBJECT_MAPPER = ApiJsonMapper.INSTANCE.get();

  private final Optional<Header> header;
  private final Optional<DataRow> row;
  private final Optional<KsqlErrorMessage> errorMessage;
  private final Optional<String> finalMessage;
  private final Optional<KsqlHostInfoEntity> sourceHost;

  /**
   * The header used in pull queries currently marks any primary key columns in the projection as
   * {@code KEY} columns in the {@code schema} field of the header.
   *
   * @param queryId the id of the query
   * @param schema the schema of the result
   * @return the header row.
   */
  public static StreamedRow pullHeader(
      final QueryId queryId,
      final LogicalSchema schema
  ) {
    return new StreamedRow(
        Optional.of(Header.of(queryId, Optional.empty(), schema)),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
  }

  /**
   * The header used in push queries currently does not mark any columns in the projection as {@code
   * KEY} columns.
   *
   * @param queryId the id of the query
   * @param key the ordered list of columns in the primary key of the result
   * @param values the ordered list of the columns in the projection
   * @return the header row.
   */
  public static StreamedRow pushHeader(
      final QueryId queryId,
      final List<? extends SimpleColumn> key,
      final List<? extends SimpleColumn> values
  ) {
    final Optional<List<? extends SimpleColumn>> keySchema = key.isEmpty()
        ? Optional.empty()
        : Optional.of(key);

    // Cheeky overloaded use of LogicalSchema to only represent the schema of the selected columns:
    final LogicalSchema columnsSchema = LogicalSchema.builder()
        .valueColumns(values)
        .build();

    return new StreamedRow(
        Optional.of(Header.of(queryId, keySchema, columnsSchema)),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
  }

  /**
   * Row returned from a push query table.
   */
  public static StreamedRow tableRow(
      final List<?> key,
      final GenericRow value
  ) {
    if (key.isEmpty()) {
      throw new IllegalArgumentException("Must provide key");
    }

    return new StreamedRow(
        Optional.empty(),
        Optional.of(DataRow.row(Optional.of(key), value.values())),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
  }

  /**
   * Row returned from a push query stream.
   */
  public static StreamedRow streamRow(final GenericRow value) {
    return new StreamedRow(
        Optional.empty(),
        Optional.of(DataRow.row(Optional.empty(), value.values())),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
  }

  /**
   * Row returned from a pull query.
   */
  public static StreamedRow pullRow(
      final GenericRow value,
      final Optional<KsqlHostInfoEntity> sourceHost
  ) {
    return new StreamedRow(
        Optional.empty(),
        Optional.of(DataRow.row(Optional.empty(), value.values())),
        Optional.empty(),
        Optional.empty(),
        sourceHost
    );
  }

  public static StreamedRow tombstone(final List<?> key) {
    return new StreamedRow(
        Optional.empty(),
        Optional.of(DataRow.tombstone(key)),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
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
      @JsonProperty("row") final Optional<DataRow> row,
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

  public Optional<DataRow> getRow() {
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
        && Objects.equals(finalMessage, that.finalMessage)
        && Objects.equals(sourceHost, that.sourceHost);
  }

  @Override
  public int hashCode() {
    return Objects.hash(header, row, errorMessage, finalMessage, sourceHost);
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
  public abstract static class BaseRow {

    @Override
    public String toString() {
      try {
        return OBJECT_MAPPER.writeValueAsString(this);
      } catch (final JsonProcessingException e) {
        return super.toString();
      }
    }
  }

  // Note: Type used `LogicalSchema` as a cheeky way of (de)serializing lists of columns.
  @JsonInclude(Include.NON_EMPTY)
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class Header extends BaseRow {

    private final QueryId queryId;
    private final Optional<ImmutableList<SimpleColumn>> keySchema;
    private final LogicalSchema columnsSchema;

    public static Header of(
        final QueryId queryId,
        final Optional<List<? extends SimpleColumn>> key,
        final LogicalSchema columnsSchema
    ) {
      final Optional<List<? extends SimpleColumn>> keySchema = key
          .map(k -> LogicalSchema.builder().valueColumns(k).build())
          .map(LogicalSchema::columns);

      return new Header(queryId, keySchema, columnsSchema);
    }

    public QueryId getQueryId() {
      return queryId;
    }

    /**
     * Used for push queries to return the schema of the key columns.
     *
     * <p>Note: The columns that make up the key may or may not be present in the projection, i.e.
     * in the {@link #getColumnsSchema()}.
     *
     * @return the columns that make up the key.
     */
    @JsonIgnore
    public Optional<List<SimpleColumn>> getKeySchema() {
      return keySchema.map(v -> v);
    }

    /**
     * @return The schema of the columns being returned by the query.
     */
    @JsonProperty("schema")
    public LogicalSchema getColumnsSchema() {
      return columnsSchema;
    }

    @JsonProperty("key")
    @SuppressWarnings("unused") // Invoked by reflection by Jackson.
    private Optional<LogicalSchema> getSerializedKeySchema() {
      // Type used Logical Schema to serialize key columns.
      // Note use of `valueColumns` rather than `keyColumns`, so that the list of key columns
      // does not include the `KEY` keyword.
      return keySchema
          .map(key -> LogicalSchema.builder().valueColumns(key).build());
    }

    @JsonCreator
    @SuppressWarnings("unused") // Invoked by reflection by Jackson.
    private static Header jsonCreator(
        @JsonProperty(value = "queryId", required = true) final QueryId queryId,
        @JsonProperty(value = "key") final Optional<LogicalSchema> keysSchema,
        @JsonProperty(value = "schema", required = true) final LogicalSchema columnsSchema
    ) {
      return new Header(queryId, keysSchema.map(LogicalSchema::value), columnsSchema);
    }

    private Header(
        final QueryId queryId,
        final Optional<List<? extends SimpleColumn>> keySchema,
        final LogicalSchema columnsSchema
    ) {
      this.queryId = requireNonNull(queryId, "queryId");
      this.keySchema = requireNonNull(keySchema, "keySchema")
          .map(ImmutableList::copyOf);
      this.columnsSchema = requireNonNull(columnsSchema, "columnsSchema");
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
          && Objects.equals(keySchema, header.keySchema)
          && Objects.equals(columnsSchema, header.columnsSchema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(queryId, keySchema, columnsSchema);
    }
  }

  @JsonInclude(Include.NON_EMPTY)
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class DataRow extends BaseRow {

    @EffectivelyImmutable
    private final Optional<List<?>> key;
    @EffectivelyImmutable
    private final Optional<List<?>> columns;

    public static DataRow row(
        final Optional<List<?>> key,
        final List<?> columns
    ) {
      return new DataRow(key, Optional.of(columns));
    }

    public static DataRow tombstone(
        final List<?> key
    ) {
      return new DataRow(Optional.of(key), Optional.empty());
    }


    public Optional<List<?>> getKey() {
      return key;
    }

    public Optional<List<?>> getColumns() {
      return columns;
    }

    public Optional<Boolean> getTombstone() {
      return columns.isPresent() ? Optional.empty() : Optional.of(true);
    }

    @JsonCreator
    private DataRow(
        @JsonProperty(value = "key") final Optional<List<?>> key,
        @JsonProperty(value = "columns") final Optional<List<?>> columns
    ) {
      this.key = requireNonNull(key, "key")
          .map(ArrayList::new)
          .map(Collections::unmodifiableList);
      this.columns = requireNonNull(columns, "columns")
          .map(ArrayList::new)
          .map(Collections::unmodifiableList);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final DataRow row = (DataRow) o;
      return Objects.equals(key, row.key)
          && Objects.equals(columns, row.columns);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, columns);
    }
  }
}
