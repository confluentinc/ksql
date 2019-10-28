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

package io.confluent.ksql.function.udf;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;

@UdtfDescription(name = "test_udtf", description = "test")
@SuppressWarnings("unused")
public class TestUdtf {

  @Udtf
  public List<String> standardParams(
      int i, long l, double d, boolean b, String s,
      BigDecimal bd, @UdfParameter(schema = "STRUCT<A VARCHAR>") Struct struct
  ) {
    return ImmutableList.of(String.valueOf(i), String.valueOf(l), String.valueOf(d),
        String.valueOf(b), s, bd.toString(), struct.toString()
    );
  }

  @Udtf
  public List<String> parameterizedListParams(
      List<Integer> i, List<Long> l, List<Double> d, List<Boolean> b, List<String> s,
      List<BigDecimal> bd, @UdfParameter(schema = "ARRAY<STRUCT<A VARCHAR>>") List<Struct> struct
  ) {
    return ImmutableList
        .of(String.valueOf(i.get(0)), String.valueOf(l.get(0)), String.valueOf(d.get(0)),
            String.valueOf(b.get(0)), s.get(0), bd.get(0).toString(), struct.get(0).toString()
        );
  }

  @Udtf
  public List<String> parameterizedMapParams(
      Map<String, Integer> i, Map<String, Long> l, Map<String, Double> d, Map<String, Boolean> b,
      Map<String, String> s,
      Map<String, BigDecimal> bd,
      @UdfParameter(schema = "MAP<STRING, STRUCT<A VARCHAR>>") Map<String, Struct> struct
  ) {
    return ImmutableList
        .of(
            String.valueOf(i.values().iterator().next()),
            String.valueOf(l.values().iterator().next()),
            String.valueOf(d.values().iterator().next()),
            String.valueOf(b.values().iterator().next()),
            s.values().iterator().next(),
            bd.values().iterator().next().toString(),
            struct.values().iterator().next().toString()
        );
  }

  @Udtf
  public List<Integer> listIntegerReturn(int i) {
    return ImmutableList.of(i);
  }

  @Udtf
  public List<Long> listLongReturn(long l) {
    return ImmutableList.of(l);
  }

  @Udtf
  public List<Double> listDoubleReturn(double d) {
    return ImmutableList.of(d);
  }

  @Udtf
  public List<Boolean> listBooleanReturn(boolean b) {
    return ImmutableList.of(b);
  }

  @Udtf
  public List<String> listStringReturn(String s) {
    return ImmutableList.of(s);
  }

  @Udtf(schema = "DECIMAL(10, 10)")
  public List<BigDecimal> listBigDecimalReturn(BigDecimal bd) {
    return ImmutableList.of(bd);
  }

  @Udtf(schema = "STRUCT<A VARCHAR>")
  public List<Struct> listStructReturn(@UdfParameter(schema = "STRUCT<A VARCHAR>") Struct struct) {
    return ImmutableList.of(struct);
  }

}
