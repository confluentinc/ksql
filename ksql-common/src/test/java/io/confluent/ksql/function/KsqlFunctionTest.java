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

package io.confluent.ksql.function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlFunctionTest {

  @Mock
  private Function<KsqlConfig, Kudf> udfFactory;

  @Test
  public void shouldThrowOnNonOptionalReturnType() {
    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> KsqlFunction.create(
            Schema.INT32_SCHEMA, // <-- non-optional return type.
            Collections.emptyList(),
            "funcName",
            MyUdf.class,
            udfFactory,
            "the description",
            "path/udf/loaded/from.jar",
            false
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("KSQL only supports optional field types"));
  }

  private static final class MyUdf implements Kudf {

    @Override
    public Object evaluate(final Object... args) {
      return null;
    }
  }
}