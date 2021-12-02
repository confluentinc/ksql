/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Strings;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.Optional;

/**
 * A set of column constraints found in a stream or table column.
 */
@Immutable
public final class ColumnConstraints {
  public static final ColumnConstraints NO_COLUMN_CONSTRAINTS = (new Builder()).build();

  private final boolean primaryKey;
  private final boolean key;
  private final boolean headers;
  private final Optional<String> headerKey;

  private ColumnConstraints(
      final boolean primaryKey,
      final boolean key,
      final boolean headers,
      final Optional<String> headerKey
  ) {
    this.primaryKey = primaryKey;
    this.key = key;
    this.headers = headers;
    this.headerKey = headerKey;
  }

  public boolean isPrimaryKey() {
    return primaryKey;
  }

  public boolean isKey() {
    return key;
  }

  public boolean isHeaders() {
    return headers;
  }

  public Optional<String> getHeaderKey() {
    return headerKey;
  }

  @Override
  public int hashCode() {
    return Objects.hash(primaryKey, key, headerKey, headerKey);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final ColumnConstraints o = (ColumnConstraints) obj;
    return Objects.equals(this.primaryKey, o.primaryKey)
        && Objects.equals(this.key, o.key)
        && Objects.equals(this.headers, o.headers)
        && Objects.equals(this.headerKey, o.headerKey);
  }

  @Override
  public String toString() {
    return "ColumnConstraints{"
        + "primaryKey=" + primaryKey
        + ", key=" + key
        + ", headers=" + headers
        + ", headerKey=" + headerKey
        + '}';
  }

  public static class Builder {
    private boolean primaryKey = false;
    private boolean key = false;
    private boolean headers = false;
    private Optional<String> headerKey = Optional.empty();

    public Builder primaryKey() {
      checkArgument(!key && !headers,
          "Primary key cannot be used if a key or headers constraint are already defined");
      this.primaryKey = true;
      return this;
    }

    public Builder key() {
      checkArgument(!primaryKey && !headers,
          "Key cannot be used if a primary key or headers constraint are already defined");
      this.key = true;
      return this;
    }

    public Builder headers() {
      checkArgument(!primaryKey && !key,
          "Headers cannot be used if a key is already defined");
      this.headers = true;
      return this;
    }

    public Builder header(final String headerKey) {
      checkArgument(!primaryKey && !key,
          "Headers columns cannot be used if a key is already defined");
      checkNotNull(Strings.emptyToNull(headerKey),
          "Header column must have a non-empty header key name defined");

      this.headers = true;
      this.headerKey = Optional.of(headerKey);
      return this;
    }

    public ColumnConstraints build() {
      return new ColumnConstraints(primaryKey, key, headers, headerKey);
    }
  }
}
