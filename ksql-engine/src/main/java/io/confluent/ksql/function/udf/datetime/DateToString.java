package io.confluent.ksql.function.udf.datetime;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@UdfDescription(name = "datetostring", author = "Confluent",
    description = "Converts an integer representing days since epoch to a date string using the given format pattern."
        + " Note this is the format Kafka Connect uses to represent dates with no time component."
        + " The format pattern should be in the format expected by java.time.format.DateTimeFormatter")
public class DateToString {

  private final LoadingCache<String, DateTimeFormatter> formatters =
      CacheBuilder.newBuilder()
        .maximumSize(1000)
        .build(CacheLoader.from(DateTimeFormatter::ofPattern));

  @Udf(description = "Converts an integer representing days since epoch to a string using the given format pattern."
      + " The format pattern should be in the format expected by java.time.format.DateTimeFormatter")
  public String dateToString(final Integer daysSinceEpoch, final String formatPattern) {
    final DateTimeFormatter formatter = formatters.getUnchecked(formatPattern);
    return LocalDate.ofEpochDay(daysSinceEpoch).format(formatter);
  }

}
