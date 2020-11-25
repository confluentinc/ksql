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

package io.confluent.ksql.execution.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.Test;

public class KeyUtilTest {

  @Test
  public void shouldConvertNonWindowedKeyToList() {
    // Given:
    final GenericKey key = GenericKey.genericKey(10);

    // When:
    final List<?> result = KeyUtil.asList(key);

    // Then:
    assertThat(result, is(ImmutableList.of(10)));
  }

  @Test
  public void shouldConvertNullKeyToList() {
    // Given:
    final GenericKey key = GenericKey.genericKey((Object)null);

    // When:
    final List<?> result = KeyUtil.asList(key);

    // Then:
    assertThat(result, is(Collections.singletonList((null))));
  }

  @Test
  public void shouldConvertWindowedKeyToList() {
    // Given:
    final Windowed<GenericKey> key = new Windowed<>(
        GenericKey.genericKey(10),
        new TimeWindow(1000, 2000)
    );

    // When:
    final List<?> result = KeyUtil.asList(key);

    // Then:
    assertThat(result, is(ImmutableList.of(10, 1000L, 2000L)));
  }
}