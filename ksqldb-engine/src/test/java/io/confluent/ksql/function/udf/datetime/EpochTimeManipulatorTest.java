package io.confluent.ksql.function.udf.datetime;

import static org.junit.Assert.assertEquals;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class EpochTimeManipulatorTest {
  private static final Logger LOG = LoggerFactory.getLogger(EpochTimeManipulatorTest.class);

  EpochTimeManipulator epochTimeManipulator = new EpochTimeManipulator();

  @Test
  public void name() {
    // 2020-03-17T22:25:45.452+0000
    final long dtUtc = ZonedDateTime.of(2020, 3, 17, 22, 25, 
        45, 452, ZoneId.of("Europe/Berlin")).toEpochSecond() * 1000L;
    final Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(dtUtc);
    LOG.debug("Millis UTC: " + dtUtc + " - Date UTC: " + calendar.getTime());

    final long dtMuc = epochTimeManipulator.epochTimeOfTimeZone(dtUtc, "Europe/Berlin");
    calendar.setTimeInMillis(dtMuc);
    LOG.debug("Millis MUC: " + dtMuc + " - Date MUC: " + calendar.getTime());
  }

  @Test
  public void epochTimeOfTimeZone() {

    assertEquals(ZonedDateTime.of(2019, 11, 12, 14, 55, 1, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L + 66,
        epochTimeManipulator.epochTimeOfTimeZone(1573566901066L, "Europe/Berlin"));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 14, 55, 7, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L + 304,
        epochTimeManipulator.epochTimeOfTimeZone(1573566907304L, "Europe/Berlin"));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 14, 54, 57, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L + 550,
        epochTimeManipulator.epochTimeOfTimeZone(1573566897550L, "Europe/Berlin"));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 14, 54, 53, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L,
        epochTimeManipulator.epochTimeOfTimeZone(1573566893000L, "Europe/Berlin"));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 15, 6, 10, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L + 8,
        epochTimeManipulator.epochTimeOfTimeZone(1573567570008L, "Europe/Berlin"));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 14, 6, 10, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L + 8,
        epochTimeManipulator.epochTimeOfTimeZone(1573567570008L, "europe/berlin"));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 14, 6, 10, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L + 8,
        epochTimeManipulator.epochTimeOfTimeZone(1573567570008L, "EUROPE/BERLIN"));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 14, 55, 7, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L,
        epochTimeManipulator.epochTimeOfTimeZone(1573566907000L, "CET"));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 16, 55, 7, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L,
        epochTimeManipulator.epochTimeOfTimeZone(1573566907000L, "Europe/Moscow"));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 8, 55, 7, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L,
        epochTimeManipulator.epochTimeOfTimeZone(1573566907000L, "America/New_York"));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 13, 55, 7, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L,
        epochTimeManipulator.epochTimeOfTimeZone(1573566907000L, "America/NewYork"));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 13, 55, 7, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L,
        epochTimeManipulator.epochTimeOfTimeZone(1573566907000L, "UTC"));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 13, 55, 7, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L,
        epochTimeManipulator.epochTimeOfTimeZone(1573566907000L, " UTC "));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 13, 55, 7, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L,
        epochTimeManipulator.epochTimeOfTimeZone(1573566907000L, ""));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 13, 55, 7, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L,
        epochTimeManipulator.epochTimeOfTimeZone(1573566907000L, "   "));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 13, 55, 7, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L,
        epochTimeManipulator.epochTimeOfTimeZone(1573566907000L, null));

    assertEquals(ZonedDateTime.of(2019, 11, 12, 13, 55, 7, 
        0, ZoneId.of("UTC")).toEpochSecond() * 1000L,
        epochTimeManipulator.epochTimeOfTimeZone(1573566907000L, "null"));

    assertEquals(ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, 
        ZoneId.of("UTC")).toEpochSecond(),
        epochTimeManipulator.epochTimeOfTimeZone(0L, "UTC"));

    assertEquals(ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, 
        ZoneId.of("UTC")).toEpochSecond(),
        epochTimeManipulator.epochTimeOfTimeZone(0L, "wrong Time Zone  !"));
  }
}