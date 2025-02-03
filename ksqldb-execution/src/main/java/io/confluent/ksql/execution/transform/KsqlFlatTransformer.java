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

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericRow;
import java.util.Collections;
import java.util.Optional;

/**
 * A transformer that wraps another transformer and flattens the result.
 * @param <K> the type of the key
 */
public class KsqlFlatTransformer<K, R> implements KsqlTransformer<K, Iterable<R>> {
  private final KsqlTransformer<K, Optional<R>> delegate;

  public KsqlFlatTransformer(final KsqlTransformer<K, Optional<R>> delegate) {
    this.delegate = requireNonNull(delegate, "delegate");
  }

  @Override
  public Iterable<R> transform(final K readOnlyKey, final GenericRow value) {
    final Optional<R> result = delegate.transform(readOnlyKey, value);
    return result
        .map(Collections::singletonList)
        .orElse(Collections.emptyList());
  }
}
