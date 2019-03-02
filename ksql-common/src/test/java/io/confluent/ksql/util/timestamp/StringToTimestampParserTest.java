package io.confluent.ksql.util.timestamp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import org.junit.Test;

public class StringToTimestampParserTest {

  @Test
  public void shouldParseFullTimestamp() {
    // Given:
    final StringToTimestampParser parser = new StringToTimestampParser("yyyy/MM/dd:HH");

    // When:
    final long ts = parser.parse("1605/11/05:12");

    // Then:
    assertThat(ts, equalTo(toMillis(LocalDateTime.of(1605, 11, 5, 12, 0))));
  }

  @Test
  public void shouldParseFullTimestampWithMinutes() {
    // Given:
    final StringToTimestampParser parser = new StringToTimestampParser("yyyy/MM/dd HH:mm");

    // When:
    final long ts = parser.parse("1605/11/05 12:10");

    // Then:
    assertThat(ts, equalTo(toMillis(LocalDateTime.of(1605, 11, 5, 12, 10))));
  }

  @Test
  public void shouldParseWithOptionalElements() {
    // Given:
    final StringToTimestampParser parser = new StringToTimestampParser("yyyy/MM/dd HH[:mm]");

    // When:
    final long ts = parser.parse("1605/11/05 12");

    // Then:
    assertThat(ts, equalTo(toMillis(LocalDateTime.of(1605, 11, 5, 12, 0))));
  }

  @Test
  public void shouldParseWithOptionalDefaultedFields() {
    // Given:
    final StringToTimestampParser parser = new StringToTimestampParser("yyyy/MM[/dd]");

    // When:
    final long ts = parser.parse("1605/11/05");

    // Then:
    assertThat(ts, equalTo(toMillis(LocalDateTime.of(1605, 11, 5, 0, 0))));
  }

  @Test
  public void shouldParseClockHourOfDay() {
    // Given:
    final StringToTimestampParser parser = new StringToTimestampParser("yyyy/MM/dd[ k]");

    // When:
    final long ts = parser.parse("1605/11/01 2");

    // Then:
    assertThat(ts, equalTo(toMillis(LocalDateTime.of(1605, 11, 1, 2, 0))));
  }

  @Test
  public void shouldResolveDefaultsWithNoFormat() {
    // Given:
    final StringToTimestampParser parser = new StringToTimestampParser("");

    // When:
    final long ts = parser.parse("");

    // Then:
    assertThat(ts, equalTo(toMillis(LocalDateTime.of(1970, 1, 1, 0, 0))));
  }

  @Test
  public void shouldResolveDefaultsWithPartialFormat() {
    // Given:
    final StringToTimestampParser parser = new StringToTimestampParser("yyyy");

    // When:
    final long ts = parser.parse("2010");

    // Then:
    assertThat(ts, equalTo(toMillis(LocalDateTime.of(2010, 1, 1, 0, 0))));
  }

  @Test
  public void shouldResolveDefaultsWithGapFormat() {
    // Given:
    final StringToTimestampParser parser = new StringToTimestampParser("yyyy-dd");

    // When:
    final long ts = parser.parse("2010-10");

    // Then:
    assertThat(ts, equalTo(toMillis(LocalDateTime.of(2010, 1, 10, 0, 0))));
  }

  @Test
  public void shouldNotResolveDefaultsForConflictingFormats() {
    // Given:
    final StringToTimestampParser parser = new StringToTimestampParser("yyyy-DDD");

    // When:
    final int fifthOfNovember = LocalDateTime.of(1605, 11, 5, 0, 0).getDayOfYear();
    final long ts = parser.parse(String.format("%d-%3d", 1605, fifthOfNovember));

    // Then:
    assertThat(ts, equalTo(toMillis(LocalDateTime.of(1605, 11, 5, 0, 0))));
  }

  @Test
  public void shouldResolveDefaultsForOptionalFields() {
    // Given:
    final StringToTimestampParser parser = new StringToTimestampParser("yyyy/MM[/dd]");

    // When:
    final long ts = parser.parse("1605/11");

    // Then:
    assertThat(ts, equalTo(toMillis(LocalDateTime.of(1605, 11, 1, 0, 0))));
  }

  @Test
  public void shouldResolveDefaultsForClockHourOfDay() {
    // Given:
    final StringToTimestampParser parser = new StringToTimestampParser("yyyy/MM/dd[ k]");

    // When:
    final long ts = parser.parse("1605/11/01");

    // Then:
    assertThat(ts, equalTo(toMillis(LocalDateTime.of(1605, 11, 1, 0, 0))));
  }

  private long toMillis(LocalDateTime time) {
    return Timestamp.valueOf(time).getTime();
  }

}