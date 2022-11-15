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

package io.confluent.ksql.planner.plan;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.util.GrammaticalJoiner;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * A node that collaborates in joins.
 */
public interface JoiningNode {

  /**
   * Get any <b>required</b> key format.
   *
   * <p>Table's, which don't yet support changing key format, <i>require</i> the join key format
   * to be their key format.
   *
   * @return the required key format, if there is one.
   */
  Optional<RequiredFormat> getRequiredKeyFormat();

  /**
   * Get any <b>preferred</b> key format.
   *
   * <p>Any side that is already being repartitioned has no preferred key format.
   *
   * @return the preferred key format, if there is one.
   */
  Optional<FormatInfo> getPreferredKeyFormat();

  /**
   * Set the key format.
   *
   * @param format the key format.
   */
  void setKeyFormat(FormatInfo format);

  @Immutable
  class RequiredFormat {

    private final FormatInfo format;
    private final ImmutableSet<SourceName> sourceNames;

    public static RequiredFormat of(final FormatInfo format, final SourceName sourceName) {
      return new RequiredFormat(format, ImmutableSet.of(sourceName));
    }

    private RequiredFormat(final FormatInfo format, final ImmutableSet<SourceName> sourceNames) {
      this.format = requireNonNull(format, "format");
      this.sourceNames = requireNonNull(sourceNames, "sourceNames");
    }

    public FormatInfo format() {
      return format;
    }

    public RequiredFormat add(final FormatInfo format, final SourceName sourceName) {
      return merge(of(format, sourceName));
    }

    public RequiredFormat merge(final RequiredFormat other) {
      throwOnMismatch("formats", other, FormatInfo::getFormat);
      throwOnMismatch("format properties", other, FormatInfo::getProperties);

      return new RequiredFormat(
          format,
          ImmutableSet.<SourceName>builder().addAll(sourceNames).addAll(other.sourceNames).build()
      );
    }

    private void throwOnMismatch(
        final String part,
        final RequiredFormat other,
        final Function<FormatInfo, ?> getter
    ) {
      final Object value = getter.apply(format);
      final Object otherValue = getter.apply(other.format);
      if (Objects.equals(value, otherValue)) {
        return;
      }

      final String names = GrammaticalJoiner.and().join(sourceNames);
      final String otherNames = GrammaticalJoiner.and().join(other.sourceNames);

      final String has = sourceNames.size() > 1 ? " have " : " has ";
      final String otherHas = other.sourceNames.size() > 1 ? " have " : " has ";

      throw new KsqlException("Incompatible key " + part + ". "
          + names + has + value + " while " + otherNames + otherHas + otherValue + "."
          + System.lineSeparator()
          + "Correct the key format by creating a copy of the table with the correct key format. "
          + "For example:"
          + System.lineSeparator()
          + "\tCREATE TABLE T_COPY"
          + System.lineSeparator()
          + "\t WITH (KEY_FORMAT = <required format>, <other key format config>)"
          + System.lineSeparator()
          + "\t AS SELECT * FROM T;"
      );
    }
  }
}
