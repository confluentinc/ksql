/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import org.junit.Test;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

public class ArrayUtilTest {

  @Test
  public void shouldGetCorrectNullIndex() {
    Double[] doubles1 = new Double[]{10.0, null, null};
    Double[] doubles2 = new Double[]{null, null, null};
    Double[] doubles3 = new Double[]{10.0, 9.0, 8.0};

    assertThat(ArrayUtil.getNullIndex(doubles1), equalTo(1));
    assertThat(ArrayUtil.getNullIndex(doubles2), equalTo(0));
    assertThat(ArrayUtil.getNullIndex(doubles3), equalTo(-1));
  }

  @Test
  public void shouldPadWithNullCorrectly() {
    Double[] doubles1 = new Double[]{10.0, null, null};
    Double[] doubles2 = new Double[]{null, null, null};
    Double[] doubles3 = new Double[]{10.0, 9.0, 8.0};

    assertThat(ArrayUtil.padWithNull(Double.class, doubles1, 5), equalTo(new Double[]{10.0,
                                                                                      null, null,
                                                                                      null, null}));
    assertThat(ArrayUtil.padWithNull(Double.class, doubles2, 5), equalTo(new Double[]{null, null,
                                                                                      null, null, null}));
    assertThat(ArrayUtil.padWithNull(Double.class, doubles3, 5), equalTo(new Double[]{10.0, 9.0, 8.0, null, null}));
    assertThat(ArrayUtil.padWithNull(Double.class, doubles3, 2), equalTo(new Double[]{10.0, 9.0, 8.0}));
  }

}
