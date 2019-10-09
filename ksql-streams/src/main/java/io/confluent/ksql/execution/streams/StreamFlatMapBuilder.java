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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.function.udtf.KudtfFlatMapper;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamFlatMap;

public final class StreamFlatMapBuilder {

  private StreamFlatMapBuilder() {
  }

  public static <K> KStreamHolder<K> build(
      final KStreamHolder<K> stream,
      final StreamFlatMap<K> step,
      final KsqlQueryBuilder queryBuilder) {

    final KudtfFlatMapper flatMapper = new KudtfFlatMapper(step.getFunctionCalls(),
        step.getInputSchema(),
        step.getOutputSchema(), step.getFunctionRegistry());

    return stream.withStream(stream.getStream().flatMapValues(flatMapper));
  }

}