/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.function.udaf.min;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udf.string.FromBytes;
import io.confluent.ksql.function.udf.string.ToBytes;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.BytesUtils.Encoding;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;

public class BytesMinKudafTest {

  private ToBytes toBytesUDF;
  private FromBytes fromBytesUDF;

  @Before
  public void setUp() {
    toBytesUDF = new ToBytes();
    fromBytesUDF = new FromBytes();
  }

  @Test
  public void shouldFindCorrectMin() {
    final MinKudaf<ByteBuffer> bytesMinKudaf = getBytesMinKudaf();
    final String[] values = new String[]{"C", "F", "B", "E", "A", "D", "B"};
    String currentMin = "D";
    for (final String val : values) {
      currentMin = fromBytesUDF.fromBytes(
          bytesMinKudaf.aggregate(
              toBytesUDF.toBytes(val, Encoding.ASCII.toString()),
              toBytesUDF.toBytes(currentMin, Encoding.ASCII.toString())),
          Encoding.ASCII.toString());
    }
    assertThat("A", equalTo(currentMin));
  }

  @Test
  public void shouldHandleNull() {
    final MinKudaf<ByteBuffer> bytesMinKudaf = getBytesMinKudaf();
    final String[] values = new String[]{"C", "F", "B", "E", "A", "D", "B"};
    String currentMin = null;

    // null before any aggregation
    currentMin = fromBytesUDF.fromBytes(
        bytesMinKudaf.aggregate(
            null,
            toBytesUDF.toBytes(currentMin, Encoding.ASCII.toString())),
        Encoding.ASCII.toString());
    assertThat(null, equalTo(currentMin));

    // now send each value to aggregation and verify
    for (final String val : values) {
      currentMin = fromBytesUDF.fromBytes(
          bytesMinKudaf.aggregate(
              toBytesUDF.toBytes(val, Encoding.ASCII.toString()),
              toBytesUDF.toBytes(currentMin, Encoding.ASCII.toString())),
          Encoding.ASCII.toString());
    }
    assertThat("A", equalTo(currentMin));

    // null should not impact result
    currentMin = fromBytesUDF.fromBytes(
        bytesMinKudaf.aggregate(
            null,
            toBytesUDF.toBytes(currentMin, Encoding.ASCII.toString())),
        Encoding.ASCII.toString());
    assertThat("A", equalTo(currentMin));
  }

  @Test
  public void shouldFindCorrectMinForMerge() {
    final MinKudaf<ByteBuffer> bytesMinKudaf = getBytesMinKudaf();
    final String mergeResult1 = fromBytesUDF.fromBytes(
        bytesMinKudaf.merge(
            toBytesUDF.toBytes("B", Encoding.ASCII.toString()),
            toBytesUDF.toBytes("D", Encoding.ASCII.toString())),
        Encoding.ASCII.toString());
    assertThat(mergeResult1, equalTo("B"));
    final String mergeResult2 = fromBytesUDF.fromBytes(
        bytesMinKudaf.merge(
            toBytesUDF.toBytes("P", Encoding.ASCII.toString()),
            toBytesUDF.toBytes("F", Encoding.ASCII.toString())),
        Encoding.ASCII.toString());
    assertThat(mergeResult2, equalTo("F"));
    final String mergeResult3 = fromBytesUDF.fromBytes(
        bytesMinKudaf.merge(
            toBytesUDF.toBytes("A", Encoding.ASCII.toString()),
            toBytesUDF.toBytes("K", Encoding.ASCII.toString())),
        Encoding.ASCII.toString());
    assertThat(mergeResult3, equalTo("A"));
  }

  private MinKudaf<ByteBuffer> getBytesMinKudaf() {
    final Udaf<ByteBuffer, ByteBuffer, ByteBuffer> aggregateFunction = MinKudaf.createMinBytes();
    aggregateFunction.initializeTypeArguments(
            Collections.singletonList(SqlArgument.of(SqlTypes.BYTES))
    );
    assertThat(aggregateFunction, instanceOf(MinKudaf.class));
    return  (MinKudaf<ByteBuffer>) aggregateFunction;
  }
}