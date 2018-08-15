package io.confluent.ksql.function.udaf.sum;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.TableAggregationFunction;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public abstract class BaseSumKudafTest<
    T extends Number, AT extends TableAggregationFunction<T, T>> {
  protected interface TGenerator<TG> {
    TG fromInt(int s);
  }

  @Test
  public void shouldComputeCorrectSum() {
    final TGenerator<T> tGenerator = getTGenerator();
    final AT sumKudaf = getSumKudaf();
    T currentVal = tGenerator.fromInt(0);
    final List<T> values = Stream.of(3, 5, 8, 2, 3, 4, 5)
        .map(v -> tGenerator.fromInt(v)).collect(Collectors.toList());
    for (final T i : values) {
      currentVal = sumKudaf.aggregate(i, currentVal);
    }
    assertThat(tGenerator.fromInt(30), equalTo(currentVal));
  }

  @Test
  public void shouldComputeCorrectSubraction() {
    final TGenerator<T> tGenerator = getTGenerator();
    final AT sumKudaf = getSumKudaf();
    T currentVal = tGenerator.fromInt(30);
    final List<T> values = Stream.of(3, 5, 8, 2, 3, 4, 5)
        .map(v -> tGenerator.fromInt(v)).collect(Collectors.toList());
    for (final T i: values) {
      currentVal = sumKudaf.undo(i, currentVal);
    }
    assertThat(tGenerator.fromInt(0), equalTo(currentVal));
  }

  @Test
  public void shouldComputeCorrectSumMerge() {
    final TGenerator<T> tGenerator = getTGenerator();
    final AT sumKudaf = getSumKudaf();
    final Merger<String, T> merger = sumKudaf.getMerger();
    final T mergeResult1 = merger.apply("key", tGenerator.fromInt(10), tGenerator.fromInt(12));
    assertThat(mergeResult1, equalTo(tGenerator.fromInt(22)));
    final T mergeResult2 = merger.apply("key", tGenerator.fromInt(10), tGenerator.fromInt(-12));
    assertThat(mergeResult2, equalTo(tGenerator.fromInt(-2)));
    final T mergeResult3 = merger.apply("key", tGenerator.fromInt(-10), tGenerator.fromInt(0));
    assertThat(mergeResult3, equalTo(tGenerator.fromInt(-10)));
  }

  protected abstract TGenerator<T> getTGenerator();

  protected abstract AT getSumKudaf();
}
