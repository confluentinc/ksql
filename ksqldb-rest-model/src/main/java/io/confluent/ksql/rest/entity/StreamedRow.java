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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.schema.ksql.LogicalSchema;
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
  private final Optional<PushContinuationToken> continuationToken;
  private final Optional<ConsistencyToken> consistencyToken;

  /**
   * The header used in queries.
   *
   * <p>Pull queries currently mark any primary key columns in the projection as
   * {@code KEY} columns in the {@code schema} field of the header.
   *
   * <p>Push queries currently do not mark any key columns with {@code KEY}.
   *
   * @param queryId the id of the query
   * @param schema the schema of the result
   * @return the header row.
   */
  public static StreamedRow header(
      final QueryId queryId,
      final LogicalSchema schema
  ) {
    return new StreamedRow(
        Optional.of(Header.of(queryId, schema)),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
  }

  /**
   * Row returned from a push query.
   */
  public static StreamedRow pushRow(final GenericRow value) {
    return new StreamedRow(
        Optional.empty(),
        Optional.of(DataRow.row(value.values())),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
  }

  /**
   * Row returned from a push query.
   */
  public static StreamedRow continuationToken(final PushContinuationToken pushContinuationToken) {
    return new StreamedRow(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(pushContinuationToken),
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
        Optional.of(DataRow.row(value.values())),
        Optional.empty(),
        Optional.empty(),
        sourceHost,
        Optional.empty(),
        Optional.empty()
    );
  }

  public static StreamedRow tombstone(final GenericRow columns) {
    return new StreamedRow(
        Optional.empty(),
        Optional.of(DataRow.tombstone(columns.values())),
        Optional.empty(),
        Optional.empty(),
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
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
  }

  public static StreamedRow error(final KsqlErrorMessage errorMessage) {
    return new StreamedRow(
        Optional.empty(),
        Optional.empty(),
        Optional.of(errorMessage),
        Optional.empty(),
        Optional.empty(),
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
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
  }

  /**
   * Row that contains the serialized consistency offset vector
   */
  public static StreamedRow consistencyToken(final ConsistencyToken consistencyToken) {
    return new StreamedRow(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(consistencyToken)
    );
  }


  @JsonCreator
  private StreamedRow(
      @JsonProperty("header") final Optional<Header> header,
      @JsonProperty("row") final Optional<DataRow> row,
      @JsonProperty("errorMessage") final Optional<KsqlErrorMessage> errorMessage,
      @JsonProperty("finalMessage") final Optional<String> finalMessage,
      @JsonProperty("sourceHost") final Optional<KsqlHostInfoEntity> sourceHost,
      @JsonProperty("continuationToken") final Optional<PushContinuationToken> continuationToken,
      @JsonProperty("consistencyToken") final Optional<ConsistencyToken> consistencyToken
  ) {
    this.header = requireNonNull(header, "header");
    this.row = requireNonNull(row, "row");
    this.errorMessage = requireNonNull(errorMessage, "errorMessage");
    this.finalMessage = requireNonNull(finalMessage, "finalMessage");
    this.sourceHost = requireNonNull(sourceHost, "sourceHost");
    this.continuationToken = requireNonNull(continuationToken, "continuationToken");
    this.consistencyToken = requireNonNull(
        consistencyToken, "consistencyToken");
    checkUnion(header, row, errorMessage, finalMessage, continuationToken, consistencyToken);
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

  public Optional<PushContinuationToken> getContinuationToken() {
    return continuationToken;
  }

  public Optional<ConsistencyToken> getConsistencyToken() {
    return consistencyToken;
  }

  @JsonIgnore
  public boolean isTerminal() {
    return finalMessage.isPresent() || errorMessage.isPresent();
  }

  public StreamedRow withSourceHost(final KsqlHostInfoEntity sourceHost) {
    return new StreamedRow(
        this.header,
        this.row,
        this.errorMessage,
        this.finalMessage,
        Optional.ofNullable(sourceHost),
        this.continuationToken,
        this.consistencyToken
    );
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
        && Objects.equals(sourceHost, that.sourceHost)
        && Objects.equals(continuationToken, that.continuationToken)
        && Objects.equals(consistencyToken, that.consistencyToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(header, row, errorMessage, finalMessage, sourceHost, continuationToken,
                        consistencyToken);
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

  @JsonInclude(Include.NON_EMPTY)
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class Header extends BaseRow {

    private final QueryId queryId;
    private final LogicalSchema columnsSchema;

    public static Header of(
        final QueryId queryId,
        final LogicalSchema columnsSchema
    ) {
      return new Header(queryId, columnsSchema);
    }

    public QueryId getQueryId() {
      return queryId;
    }

    /**
     * @return The schema of the columns being returned by the query.
     */
    public LogicalSchema getSchema() {
      return columnsSchema;
    }

    @JsonCreator
    @SuppressWarnings("unused") // Invoked by reflection by Jackson.
    private static Header jsonCreator(
        @JsonProperty(value = "queryId", required = true) final QueryId queryId,
        @JsonProperty(value = "schema", required = true) final LogicalSchema columnsSchema
    ) {
      return new Header(queryId, columnsSchema);
    }

    private Header(
        final QueryId queryId,
        final LogicalSchema columnsSchema
    ) {
      this.queryId = requireNonNull(queryId, "queryId");
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
          && Objects.equals(columnsSchema, header.columnsSchema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(queryId, columnsSchema);
    }
  }

  @JsonInclude(Include.NON_EMPTY)
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class DataRow extends BaseRow {

    @EffectivelyImmutable
    private final List<?> columns;
    private final boolean tombstone;

    public static DataRow row(
        final List<?> columns
    ) {
      return new DataRow(columns, Optional.empty());
    }

    public static DataRow tombstone(
        final List<?> columns
    ) {
      return new DataRow(columns, Optional.of(true));
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "columns is unmodifiableList()")
    public List<?> getColumns() {
      return columns;
    }

    public Optional<Boolean> getTombstone() {
      return tombstone ? Optional.of(true) : Optional.empty();
    }

    @JsonCreator
    private DataRow(
        @JsonProperty(value = "columns") final List<?> columns,
        @JsonProperty(value = "tombstone") final Optional<Boolean> tombstone
    ) {
      this.tombstone = tombstone.orElse(false);
      // cannot use ImmutableList, as we need to handle `null`
      this.columns = Collections.unmodifiableList(
          new ArrayList<>(requireNonNull(columns, "columns"))
      );
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
      return tombstone == row.tombstone
          && Objects.equals(columns, row.columns);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tombstone, columns);
    }
  }
}
