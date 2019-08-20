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

  public static FormatInfo of(final Format format) {
    return FormatInfo.of(format, Optional.empty());
  }

  public static FormatInfo of(
      final Format format,
      final Optional<String> avroFullSchemaName
  ) {
    return new FormatInfo(format, avroFullSchemaName);
  }

  private FormatInfo(
      final Format format,
      final Optional<String> avroFullSchemaName
  ) {
    this.format = Objects.requireNonNull(format, "format");
    this.avroFullSchemaName = Objects.requireNonNull(avroFullSchemaName, "avroFullSchemaName");

    if (format != Format.AVRO && avroFullSchemaName.isPresent()) {
      throw new KsqlException("Full schema name only supported with AVRO format");
    }

    if (avroFullSchemaName.map(name -> name.trim().isEmpty()).orElse(false)) {
      throw new KsqlException("Schema name can not be empty");
    }
  }

  public Format getFormat() {
    return format;
  }

  public Optional<String> getAvroFullSchemaName() {
    return avroFullSchemaName;
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
        && Objects.equals(avroFullSchemaName, that.avroFullSchemaName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(format, avroFullSchemaName);
  }

  @Override
  public String toString() {
    return "FormatInfo{"
        + "format=" + format
        + ", avroFullSchemaName=" + avroFullSchemaName
        + '}';
  }
}
