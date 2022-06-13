package io.confluent.ksql.function.udaf.topkdistinct;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udf.string.ToBytes;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.BytesUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class BytesTopKDistinctKudafTest {
    private final List<String> valuesArray = ImmutableList.of("A", "D", "F", "A", "G", "H", "B", "H", "I", "E",
            "C", "H", "I");
    private final TopkDistinctKudaf<ByteBuffer> bytesTopkDistinctKudaf
            = TopKDistinctTestUtils.getTopKDistinctKudaf(3, SqlTypes.BYTES);
    private ToBytes toBytesUDF;

    @Before
    public void setUp() {
        toBytesUDF = new ToBytes();
    }

    @Test
    public void shouldAggregateTopK() {
        List<ByteBuffer> currentVal = new ArrayList<>();
        for (final String d: valuesArray) {
            currentVal = bytesTopkDistinctKudaf.aggregate(toBytes(d), currentVal);
        }

        List<ByteBuffer> expected = toBytes(ImmutableList.of("I", "H", "G"));
        assertThat("Invalid results.", currentVal, equalTo(expected));
    }

    @Test
    public void shouldAggregateTopKWithLessThanKValues() {
        List<ByteBuffer> currentVal = new ArrayList<>();
        currentVal = bytesTopkDistinctKudaf.aggregate(toBytes("I"), currentVal);

        assertThat("Invalid results.", currentVal, equalTo(toBytes(ImmutableList.of("I"))));
    }

    @Test
    public void shouldMergeTopK() {
        final List<ByteBuffer> array1 = toBytes(ImmutableList.of("D", "B", "A"));
        final List<ByteBuffer> array2 = toBytes(ImmutableList.of("E", "D", "C"));

        assertThat("Invalid results.", bytesTopkDistinctKudaf.getMerger().apply(null, array1, array2), equalTo(
                toBytes(ImmutableList.of("E", "D", "C"))));
    }

    @Test
    public void shouldMergeTopKWithNulls() {
        final List<ByteBuffer> array1 = toBytes(ImmutableList.of("B", "A"));
        final List<ByteBuffer> array2 = toBytes(ImmutableList.of("C"));

        assertThat("Invalid results.", bytesTopkDistinctKudaf.getMerger().apply(null, array1, array2), equalTo(
                toBytes(ImmutableList.of("C", "B", "A"))));
    }

    @Test
    public void shouldMergeTopKWithNullsDuplicates() {
        final List<ByteBuffer> array1 = toBytes(ImmutableList.of("B", "A"));
        final List<ByteBuffer> array2 = toBytes(ImmutableList.of("C", "B"));

        assertThat("Invalid results.", bytesTopkDistinctKudaf.getMerger().apply(null, array1, array2), equalTo(
                toBytes(ImmutableList.of("C", "B", "A"))));
    }

    @Test
    public void shouldMergeTopKWithMoreNulls() {
        final List<ByteBuffer> array1 = toBytes(ImmutableList.of("A"));
        final List<ByteBuffer> array2 = toBytes(ImmutableList.of("A"));

        assertThat("Invalid results.", bytesTopkDistinctKudaf.getMerger().apply(null, array1, array2), equalTo(
                toBytes(ImmutableList.of("A"))));
    }

    private ByteBuffer toBytes(String val) {
        return toBytesUDF.toBytes(val, BytesUtils.Encoding.ASCII.toString());
    }

    private List<ByteBuffer> toBytes(List<String> vals) {
        return vals.stream().map(this::toBytes).collect(Collectors.toList());
    }
}
