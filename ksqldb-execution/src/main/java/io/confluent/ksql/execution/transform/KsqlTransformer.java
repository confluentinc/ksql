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

package io.confluent.ksql.execution.transform;

import io.confluent.ksql.GenericRow;

/**
 * Transform step.
 *
 * <p>Decoupled from any specific implementation, e.g. Kafka streams.
 *
 * <p>Transforms a single input row: read-only key + value.
 *
 * @param <K> the type of the key
 * @param <R> the return type
 */
public interface KsqlTransformer<K, R> {

  R transform(K readOnlyKey, GenericRow value);
}
