package io.confluent.ksql.util.timestamp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.util.KsqlException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

public class StringToTimestampParserTest {

  private static final ZoneId ZID = ZoneId.systemDefault();
  private static final ZoneId GMT_3 = ZoneId.of("GMT+3");
  private static final ZoneId IGNORED = ZID;

  private static final ZonedDateTime EPOCH =
      ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZID);

  private static final ZonedDateTime FIFTH_OF_NOVEMBER =
      ZonedDateTime.of(1605, 11, 5, 0, 0, 0, 0, ZID);

  private static final ZonedDateTime NEW_YEARS_EVE_2012 =
      ZonedDateTime.of(2012, 12, 31, 23, 59, 58, 660000000, ZID);

  @Test
  public void shouldParseBasicLocalDate() {
    // Given
    final String format = "yyyy-MM-dd HH";
    final String timestamp = "1605-11-05 10";

    // When
    final ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, sameInstant(
        FIFTH_OF_NOVEMBER
            .withHour(10)
            .withZoneSameInstant(ZID)));
  }

  @Test
  public void shouldParseToTimestamp() {
    // Given
    final String format = "yyyy-MM-dd HH";
    final String timestamp = "1605-11-05 10";

    // When
    final Timestamp ts = new StringToTimestampParser(format).parseToTimestamp(timestamp, ZoneId.systemDefault());

    // Then
    assertThat(ts.getTime(), is(
        FIFTH_OF_NOVEMBER
            .withHour(10)
            .withZoneSameInstant(ZID)
            .toInstant()
            .toEpochMilli()));
  }

  @Test
  public void shouldConvertToMillis() {
    // Given
    final String format = "yyyy-MM-dd HH";
    final String timestamp = "1605-11-05 10";

    // When
    final long ts = new StringToTimestampParser(format).parse(timestamp);

    // Then
    assertThat(ts, is(
        FIFTH_OF_NOVEMBER
            .withHour(10)
            .withZoneSameInstant(ZID)
            .toInstant()
            .toEpochMilli()));
  }

  @Test
  public void shouldParseFullLocalDateWithPartialSeconds() {
    // Given
    final String format = "yyyy-MM-dd HH:mm:ss:SSS";
    final String timestamp = "1605-11-05 10:10:10:010";

    // When
    final ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(FIFTH_OF_NOVEMBER
        .withHour(10)
        .withMinute(10)
        .withSecond(10)
        .withNano(10_000_000))));
  }

  @Test
  public void shouldParseFullLocalDateWithNanoSeconds() {
    // Given
    final String format = "yyyy-MM-dd HH:mm:ss:nnnnnnnnn";
    // Note that there is an issue when resolving nanoseconds that occur below the
    // micro-second granularity. Since this is a private API (the only one exposed
    // converts it to millis) we can safely ignore it.
    final String timestamp = "1605-11-05 10:10:10:001000000";

    // When
    final ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(FIFTH_OF_NOVEMBER
        .withHour(10)
        .withMinute(10)
        .withSecond(10)
        .withNano(1_000_000))));
  }

  @Test
  public void shouldParseFullLocalDateWithOptionalElements() {
    // Given
    final String format = "yyyy-MM-dd[ HH:mm:ss]";
    final String timestamp = "1605-11-05";

    // When
    final ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(FIFTH_OF_NOVEMBER)));
  }

  @Test
  public void shouldParseMonthNameCaseInsensitively() {
    // Given
    final String format = "dd-MMM-yyyy";
    final String timestamp = "05-NoV-1605";

    // When
    final ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(FIFTH_OF_NOVEMBER)));
  }

  @Test
  public void shouldParseFullLocalDateWithPassedInTimeZone() {
    // Given
    final String format = "yyyy-MM-dd HH";
    final String timestamp = "1605-11-05 10";

    // When
    final ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, GMT_3);

    // Then
    assertThat(ts, is(sameInstant(FIFTH_OF_NOVEMBER.withHour(10).withZoneSameLocal(GMT_3))));
  }

  @Test
  public void shouldParseFullLocalDateWithTimeZone() {
    // Given
    // NOTE: a trailing space is required due to JDK bug, fixed in JDK 9b116
    // https://bugs.openjdk.java.net/browse/JDK-8154050
    final String format = "yyyy-MM-dd HH O ";
    final String timestamp = "1605-11-05 10 GMT+3 ";

    // When
    final ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, IGNORED);

    // Then
    assertThat(ts, is(sameInstant(FIFTH_OF_NOVEMBER.withHour(10).withZoneSameLocal(GMT_3))));
  }

  @Test
  public void shouldParseDateTimeWithDayOfYear() {
    // Given
    final String format = "yyyy-DDD HH";
    final String timestamp = String.format("1605-%d 10", FIFTH_OF_NOVEMBER.getDayOfYear());

    // When
    final ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(FIFTH_OF_NOVEMBER.withHour(10))));
  }

  @Test
  public void shouldParseLeapDay() {
    // Given
    final String format = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    final String timestamp = "2012-12-31T23:59:58.660";

    // When
    final ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(NEW_YEARS_EVE_2012)));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowErrorForLeapDayWithoutYear() {
    // Given
    final String format = "DDD";
    final String timestamp = "366";

    // When
    new StringToTimestampParser(format).parseZoned(timestamp, ZID);
  }

  @Test
  public void shouldResolveDefaultsForEmpty() {
    // Given
    final String format = "";
    final String timestamp = "";

    // When
    final ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(EPOCH.withZoneSameInstant(ZID))));
  }

  @Test
  public void shouldResolveDefaultsForPartial() {
    // Given
    final String format = "yyyy";
    final String timestamp = "2019";

    // When
    final ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(EPOCH.withYear(2019).withZoneSameInstant(ZID))));
  }

  @Test
  public void shouldResolveDefaultsForDayOfYear() {
    // Given
    final String format = "DDD";
    final String timestamp = "100";

    // When
    final ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(EPOCH.withDayOfYear(100).withZoneSameInstant(ZID))));
  }

  private static Matcher<ZonedDateTime> sameInstant(final ZonedDateTime other) {
    return new TypeSafeMatcher<ZonedDateTime>() {
      @Override
      protected boolean matchesSafely(final ZonedDateTime item) {
        return item.toInstant().equals(other.toInstant());
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(other.toString());
      }
    };
  }

}

