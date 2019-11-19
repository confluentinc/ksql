/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable Pojo holding standard info about a format.
 */
@Immutable
public final class FormatInfo {
  private final Format format;
  private final Optional<String> avroFullSchemaName;
  private final Optional<Delimiter> delimiter;

  public static FormatInfo of(final Format format) {
    return FormatInfo.of(format, Optional.empty(), Optional.empty());
  }

  @JsonCreator
  public static FormatInfo of(
      @JsonProperty(value = "format", required = true)
      final Format format,
      @JsonProperty("avroSchemaFullName") final Optional<String> avroFullSchemaName,
      @JsonProperty("delimiter") final Optional<Delimiter> valueDelimiter) {
    return new FormatInfo(format, avroFullSchemaName, valueDelimiter);
  }

  private FormatInfo(
      final Format format,
      final Optional<String> avroFullSchemaName,
      final Optional<Delimiter> delimiter
  ) {
    this.format = Objects.requireNonNull(format, "format");
    this.avroFullSchemaName = Objects.requireNonNull(avroFullSchemaName, "avroFullSchemaName");

    if (format != Format.AVRO && avroFullSchemaName.isPresent()) {
      throw new KsqlException("Full schema name only supported with AVRO format");
    }

    if (format == Format.AVRO
        && avroFullSchemaName.map(name -> name.trim().isEmpty()).orElse(false)) {
      throw new KsqlException("Schema name cannot be empty");
    }

    this.delimiter = Objects.requireNonNull(delimiter, "delimiter");

    if (format != Format.DELIMITED && delimiter.isPresent()) {
      throw new KsqlException("Delimeter only supported with DELIMITED format");
    }

  }

  public Format getFormat() {
    return format;
  }

  public Optional<String> getAvroFullSchemaName() {
    return avroFullSchemaName;
  }

  public Optional<Delimiter> getDelimiter() {
    return delimiter;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final FormatInfo that = (FormatInfo) o;
    return format == that.format
        && Objects.equals(avroFullSchemaName, that.avroFullSchemaName)
        && Objects.equals(delimiter, that.delimiter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(format, avroFullSchemaName, delimiter);
  }

  @Override
  public String toString() {
    return "FormatInfo{"
        + "format=" + format
        + ", avroFullSchemaName=" + avroFullSchemaName
        + ", delimiter=" + delimiter
        + '}';
  }
}
