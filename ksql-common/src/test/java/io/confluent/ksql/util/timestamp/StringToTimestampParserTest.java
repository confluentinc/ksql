package io.confluent.ksql.util.timestamp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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

  @Test
  public void shouldParseBasicLocalDate() {
    // Given
    final String format = "yyyy-MM-dd HH";
    final String timestamp = "1605-11-05 10";

    // When
    ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, sameInstant(
        FIFTH_OF_NOVEMBER
            .withHour(10)
            .withZoneSameInstant(ZID)));
  }

  @Test
  public void shouldConvertToMillis() {
    // Given
    final String format = "yyyy-MM-dd HH";
    final String timestamp = "1605-11-05 10";

    // When
    long ts = new StringToTimestampParser(format).parse(timestamp);

    // Then
    assertThat(ts, is(
        FIFTH_OF_NOVEMBER
            .withHour(10)
            .withZoneSameInstant(ZID)
            .toInstant()
            .toEpochMilli()));
  }

  @Test
  public void shouldParseFullLocalDate() {
    // Given
    final String format = "yyyy-MM-dd HH:mm";
    final String timestamp = "1605-11-05 10:10";

    // When
    ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(FIFTH_OF_NOVEMBER.withHour(10).withMinute(10))));
  }

  @Test
  public void shouldParseFullLocalDateWithOptionalElements() {
    // Given
    final String format = "yyyy-MM-dd[ HH:mm:ss]";
    final String timestamp = "1605-11-05";

    // When
    ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(FIFTH_OF_NOVEMBER)));
  }

  @Test
  public void shouldParseFullLocalDateWithPassedInTimeZone() {
    // Given
    final String format = "yyyy-MM-dd HH";
    final String timestamp = "1605-11-05 10";

    // When
    ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, GMT_3);

    // Then
    assertThat(ts, is(sameInstant(FIFTH_OF_NOVEMBER.withHour(10).withZoneSameLocal(GMT_3))));
  }

  @Test
  public void shouldParseFullLocalDateWithTimeZone() {
    // Given
    final String format = "yyyy-MM-dd HH O";
    final String timestamp = "1605-11-05 10 GMT+3";

    // When
    ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, IGNORED);

    // Then
    assertThat(ts, is(sameInstant(FIFTH_OF_NOVEMBER.withHour(10).withZoneSameLocal(GMT_3))));
  }

  @Test
  public void shouldParseDateTimeWithDayOfYear() {
    // Given
    final String format = "yyyy-DDD HH";
    final String timestamp = String.format("1605-%d 10", FIFTH_OF_NOVEMBER.getDayOfYear());

    // When
    ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(FIFTH_OF_NOVEMBER.withHour(10))));
  }

  @Test
  public void shouldResolveDefaultsForEmpty() {
    // Given
    final String format = "";
    final String timestamp = "";

    // When
    ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(EPOCH.withZoneSameInstant(ZID))));
  }

  @Test
  public void shouldResolveDefaultsForPartial() {
    // Given
    final String format = "yyyy";
    final String timestamp = "2019";

    // When
    ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(EPOCH.withYear(2019).withZoneSameInstant(ZID))));
  }

  @Test
  public void shouldResolveDefaultsForDayOfYear() {
    // Given
    final String format = "DDD";
    final String timestamp = "100";

    // When
    ZonedDateTime ts = new StringToTimestampParser(format).parseZoned(timestamp, ZID);

    // Then
    assertThat(ts, is(sameInstant(EPOCH.withDayOfYear(100).withZoneSameInstant(ZID))));
  }

  private static Matcher<ZonedDateTime> sameInstant(final ZonedDateTime other) {
    return new TypeSafeMatcher<ZonedDateTime>() {
      @Override
      protected boolean matchesSafely(ZonedDateTime item) {
        return item.toInstant().equals(other.toInstant());
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(other.toString());
      }
    };
  }

}