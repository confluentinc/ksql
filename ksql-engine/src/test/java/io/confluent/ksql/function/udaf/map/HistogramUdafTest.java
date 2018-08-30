/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.ksql.function.udaf.map;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import java.util.Map;
import org.junit.Test;
import io.confluent.ksql.function.udaf.TableUdaf;

public class HistogramUdafTest {

  @Test
  public void shouldFindCorrectMax() {
    TableUdaf<String, Map<String, Long>> udaf = HistogramUdaf.histogramString();
    Map<String, Long> agg = udaf.initialize();
    String[] values = new String[] { "foo", "bar", "foo", "foo", "baz" };
    for (String thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    assertThat(agg.entrySet(), hasSize(3));
    assertThat(agg, hasEntry("foo", 3L));
    assertThat(agg, hasEntry("bar", 1L));
    assertThat(agg, hasEntry("baz", 1L));
  }

}
