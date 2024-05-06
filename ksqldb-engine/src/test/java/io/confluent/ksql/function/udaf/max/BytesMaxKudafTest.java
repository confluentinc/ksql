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

package io.confluent.ksql.function.udaf.max;

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

public class BytesMaxKudafTest {

  private ToBytes toBytesUDF;
  private FromBytes fromBytesUDF;

  @Before
  public void setUp() {
    toBytesUDF = new ToBytes();
    fromBytesUDF = new FromBytes();
  }

  @Test
  public void shouldFindCorrectMax() {
    final MaxKudaf<ByteBuffer> bytesMaxKudaf = getMaxComparableKudaf();
    final String[] values = new String[]{"C", "F", "B", "E", "A", "D", "B"};
    String currentMax = "A";
    for (final String val : values) {
      currentMax = fromBytesUDF.fromBytes(
          bytesMaxKudaf.aggregate(
              toBytesUDF.toBytes(val, Encoding.ASCII.toString()),
              toBytesUDF.toBytes(currentMax, Encoding.ASCII.toString())),
          Encoding.ASCII.toString());
    }
    assertThat("F", equalTo(currentMax));
  }

  @Test
  public void shouldHandleNull() {
    final MaxKudaf<ByteBuffer> bytesMaxKudaf = getMaxComparableKudaf();
    final String[] values = new String[]{"C", "F", "B", "E", "A", "D", "B"};
    String currentMax = null;

    // null before any aggregation
    currentMax = fromBytesUDF.fromBytes(
        bytesMaxKudaf.aggregate(
            null,
            toBytesUDF.toBytes(currentMax, Encoding.ASCII.toString())),
        Encoding.ASCII.toString());
    assertThat(null, equalTo(currentMax));

    // now send each value to aggregation and verify
    for (final String val : values) {
      currentMax = fromBytesUDF.fromBytes(
          bytesMaxKudaf.aggregate(
              toBytesUDF.toBytes(val, Encoding.ASCII.toString()),
              toBytesUDF.toBytes(currentMax, Encoding.ASCII.toString())),
          Encoding.ASCII.toString());
    }
    assertThat("F", equalTo(currentMax));

    // null should not impact result
    currentMax = fromBytesUDF.fromBytes(
        bytesMaxKudaf.aggregate(
            null,
            toBytesUDF.toBytes(currentMax, Encoding.ASCII.toString())),
        Encoding.ASCII.toString());
    assertThat("F", equalTo(currentMax));
  }

  @Test
  public void shouldFindCorrectMaxForMerge() {
    final MaxKudaf<ByteBuffer> bytesMaxKudaf = getMaxComparableKudaf();
    final String mergeResult1 = fromBytesUDF.fromBytes(
        bytesMaxKudaf.merge(
            toBytesUDF.toBytes("B", Encoding.ASCII.toString()),
            toBytesUDF.toBytes("D", Encoding.ASCII.toString())),
        Encoding.ASCII.toString());
    assertThat(mergeResult1, equalTo("D"));
    final String mergeResult2 = fromBytesUDF.fromBytes(
        bytesMaxKudaf.merge(
            toBytesUDF.toBytes("P", Encoding.ASCII.toString()),
            toBytesUDF.toBytes("F", Encoding.ASCII.toString())),
        Encoding.ASCII.toString());
    assertThat(mergeResult2, equalTo("P"));
    final String mergeResult3 = fromBytesUDF.fromBytes(
        bytesMaxKudaf.merge(
            toBytesUDF.toBytes("A", Encoding.ASCII.toString()),
            toBytesUDF.toBytes("K", Encoding.ASCII.toString())),
        Encoding.ASCII.toString());
    assertThat(mergeResult3, equalTo("K"));
  }

  private MaxKudaf<ByteBuffer> getMaxComparableKudaf() {
    final Udaf<ByteBuffer, ByteBuffer, ByteBuffer> aggregateFunction = MaxKudaf.createMaxBytes();
    aggregateFunction.initializeTypeArguments(
            Collections.singletonList(SqlArgument.of(SqlTypes.BYTES))
    );
    assertThat(aggregateFunction, instanceOf(MaxKudaf.class));
    return  (MaxKudaf<ByteBuffer>) aggregateFunction;
  }
}