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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.StreamedRow.DataRow;
import io.confluent.ksql.rest.entity.StreamedRow.Header;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public final class StreamedRowMatchers {

  public static final class HeaderMatchers {

    private HeaderMatchers() {
    }

    public static Matcher<? super Header> header(
        final Matcher<? super QueryId> expectedQueryId,
        final Matcher<? super LogicalSchema> expectedSchema
    ) {
      return allOf(
          withQueryId(expectedQueryId),
          withSchema(expectedSchema)
      );
    }

    private static Matcher<? super Header> withQueryId(
        final Matcher<? super QueryId> expectedQueryId
    ) {
      return new FeatureMatcher<Header, QueryId>
          (expectedQueryId, "header", "header") {
        @Override
        protected QueryId featureValueOf(final Header actual) {
          return actual.getQueryId();
        }
      };
    }

    private static Matcher<? super Header> withSchema(
        final Matcher<? super LogicalSchema> expectedSchema
    ) {
      return new FeatureMatcher<Header, LogicalSchema>
          (expectedSchema, "header with schema", "schema") {
        @Override
        protected LogicalSchema featureValueOf(final Header actual) {
          return actual.getSchema();
        }
      };
    }
  }

  private StreamedRowMatchers() {
  }

  public static Matcher<? super StreamedRow> header(
      final Matcher<? super Optional<Header>> expectedHeader
  ) {
    return streamedRow(
        expectedHeader,
        is(Optional.empty()),
        is(Optional.empty()),
        is(Optional.empty())
    );
  }

  public static Matcher<? super StreamedRow> dataRow(
      final Matcher<? super Optional<DataRow>> expectedRow
  ) {
    return streamedRow(
        is(Optional.empty()),
        expectedRow,
        is(Optional.empty()),
        is(Optional.empty())
    );
  }

  public static Matcher<? super StreamedRow> errorMessage(
      final Matcher<? super Optional<KsqlErrorMessage>> expectedError
  ) {
    return streamedRow(
        is(Optional.empty()),
        is(Optional.empty()),
        expectedError,
        is(Optional.empty())
    );
  }

  public static Matcher<? super StreamedRow> finalMessage(
      final Matcher<? super Optional<String>> expectedMsg
  ) {
    return streamedRow(
        is(Optional.empty()),
        is(Optional.empty()),
        is(Optional.empty()),
        expectedMsg
    );
  }

  /**
   * Returns a matcher that will match against the supplied expected row, which ignores the queryId,
   * which is uniquely generated per-call.
   *
   * @param expected the expected row.
   * @return the matcher.
   */
  public static Matcher<? super StreamedRow> matchesRow(
      final StreamedRow expected
  ) {
    final Matcher<? super Optional<Header>> headerMatcher = expected.getHeader()
        .<Matcher<Optional<Header>>>map(header -> OptionalMatchers.of(HeaderMatchers.header(
            any(QueryId.class),
            is(header.getSchema())
        )))
        .orElse(is(Optional.empty()));

    return streamedRow(
        headerMatcher,
        is(expected.getRow()),
        is(expected.getErrorMessage()),
        is(expected.getFinalMessage())
    );
  }

  public static Matcher<? super List<? extends StreamedRow>> matchersRows(
      final List<? extends StreamedRow> expected
  ) {
    final List<Matcher<? super StreamedRow>> rowMatchers = expected.stream()
        .map(StreamedRowMatchers::matchesRow)
        .collect(Collectors.toList());

    return contains(rowMatchers);
  }

  public static Matcher<? super List<? extends StreamedRow>> matchersRowsAnyOrder(
      final List<? extends StreamedRow> expected
  ) {
    final List<Matcher<? super StreamedRow>> rowMatchers = expected.stream()
        .map(StreamedRowMatchers::matchesRow)
        .collect(Collectors.toList());

    return containsInAnyOrder(rowMatchers);
  }

  private static Matcher<? super StreamedRow> streamedRow(
      final Matcher<? super Optional<Header>> expectedHeader,
      final Matcher<? super Optional<DataRow>> expectedRow,
      final Matcher<? super Optional<KsqlErrorMessage>> expectedError,
      final Matcher<? super Optional<String>> expectedFinalMsg
  ) {
    return allOf(
        withHeader(expectedHeader),
        withValue(expectedRow),
        withErrorMessage(expectedError),
        withFinalMessage(expectedFinalMsg)
    );
  }

  private static Matcher<? super StreamedRow> withHeader(
      final Matcher<? super Optional<Header>> expectedHeader
  ) {
    return new FeatureMatcher<StreamedRow, Optional<Header>>
        (expectedHeader, "header", "header") {
      @Override
      protected Optional<Header> featureValueOf(final StreamedRow actual) {
        return actual.getHeader();
      }
    };
  }

  private static Matcher<? super StreamedRow> withValue(
      final Matcher<? super Optional<DataRow>> expectedRow
  ) {
    return new FeatureMatcher<StreamedRow, Optional<DataRow>>
        (expectedRow, "value", "value") {
      @Override
      protected Optional<DataRow> featureValueOf(final StreamedRow actual) {
        return actual.getRow();
      }
    };
  }

  private static Matcher<? super StreamedRow> withErrorMessage(
      final Matcher<? super Optional<KsqlErrorMessage>> expectedError
  ) {
    return new FeatureMatcher<StreamedRow, Optional<KsqlErrorMessage>>
        (expectedError, "error message", "error message") {
      @Override
      protected Optional<KsqlErrorMessage> featureValueOf(final StreamedRow actual) {
        return actual.getErrorMessage();
      }
    };
  }

  private static Matcher<? super StreamedRow> withFinalMessage(
      final Matcher<? super Optional<String>> finalMessage
  ) {
    return new FeatureMatcher<StreamedRow, Optional<String>>
        (finalMessage, "final message", "final message") {
      @Override
      protected Optional<String> featureValueOf(final StreamedRow actual) {
        return actual.getFinalMessage();
      }
    };
  }

  private static final class OptionalMatchers {

    private OptionalMatchers() {
    }

    public static <T> Matcher<Optional<T>> of(final Matcher<? super T> valueMatcher) {
      return new TypeSafeDiagnosingMatcher<Optional<T>>() {
        @Override
        protected boolean matchesSafely(
            final Optional<T> item,
            final Description mismatchDescription
        ) {
          if (!item.isPresent()) {
            mismatchDescription.appendText("not present");
            return false;
          }

          if (!valueMatcher.matches(item.get())) {
            valueMatcher.describeMismatch(item.get(), mismatchDescription);
            return false;
          }

          return true;
        }

        @Override
        public void describeTo(final Description description) {
          description.appendText("optional ").appendDescriptionOf(valueMatcher);
        }
      };
    }
  }
}